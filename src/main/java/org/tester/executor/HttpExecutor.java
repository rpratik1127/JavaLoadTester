package org.tester.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.ScheduledFuture;
import org.tester.control.AsyncSemaphore;
import org.tester.metrics.MetricsCollector;
import org.tester.metrics.RequestFailureReason;
import org.tester.metrics.RequestMetric;
import org.tester.model.ApiStep;
import org.tester.runtime.ResponseExtractor;
import org.tester.runtime.VariableResolver;
import org.tester.runtime.VariableStore;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Async Netty HTTP client for load testing.
 * <p>
 * Uses a process-wide event loop, per-host connection pools, and an async in-flight
 * semaphore. Supports {@link ConnectionMode#POOLED} (default) and
 * {@link ConnectionMode#STICKY} (per-user serialized channels).
 */
public class HttpExecutor {

    private static final boolean DEBUG_HTTP = false;

    // --- Connection and concurrency limits ---
    private static final int MAX_CONNECTIONS_PER_HOST = 20_000;
    private static final int MAX_PENDING_ACQUIRES = 50_000;
    private static final int MAX_IN_FLIGHT_REQUESTS = 50_000;

    // --- Timeouts (milliseconds) ---
    private static final int ACQUIRE_TIMEOUT_MILLIS = 90_000;
    private static final int CONNECT_TIMEOUT_MILLIS = 20_000;
    private static final int SSL_HANDSHAKE_TIMEOUT_MILLIS = 30_000;
    /** Requests exceeding this duration fail with {@link RequestFailureReason#TIMEOUT}. */
    private static final int REQUEST_TIMEOUT_MILLIS = 60_000;

    /** Tuned for typical JSON API payloads; oversized bodies fail fast at the aggregator. */
    private static final int MAX_RESPONSE_SIZE_BYTES = 256 * 1024;

    private static final int MAX_WRITE_RETRIES = 1;

    /** Limits concurrent HTTP operations across all virtual users. */
    private static final AsyncSemaphore inFlightLimiter =
            new AsyncSemaphore(MAX_IN_FLIGHT_REQUESTS);

    private static final AttributeKey<PendingRequest> PENDING_REQUEST =
            AttributeKey.valueOf("pendingRequest");

    private static final int EVENT_LOOP_THREADS =
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2);

    private static final EventLoopGroup eventLoopGroup =
            new NioEventLoopGroup(EVENT_LOOP_THREADS);

    // =========================================================================
    // Core Executor Configuration & State
    // =========================================================================

    private static volatile ConnectionMode connectionMode = ConnectionMode.POOLED;

    private static final ExecutorService extractionExecutor = Executors.newWorkStealingPool(
            Math.max(2, Runtime.getRuntime().availableProcessors())
    );

    private static final ConcurrentHashMap<String, FixedChannelPool> poolCache =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, UserChannelLease> userChannelCache =
            new ConcurrentHashMap<>();

    /** Prevents duplicate lease creation for the same user+host key. */
    private static final ConcurrentHashMap<String, CompletableFuture<UserChannelLease>> leaseInFlight =
            new ConcurrentHashMap<>();

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final SslContext SSL_CONTEXT = createSslContext();

    private static final VariableResolver VARIABLE_RESOLVER = new VariableResolver();
    private static final ResponseExtractor RESPONSE_EXTRACTOR = new ResponseExtractor();

    private final MetricsCollector metricsCollector;

    // =========================================================================
    // Initialization & Utility Accessors
    // =========================================================================

    public HttpExecutor() {
        this.metricsCollector = null;
    }

    public HttpExecutor(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    static boolean isDebugEnabled() {
        return DEBUG_HTTP;
    }

    public static void setConnectionMode(ConnectionMode mode) {
        connectionMode = mode == null ? ConnectionMode.POOLED : mode;
    }

    public static ConnectionMode getConnectionMode() {
        return connectionMode;
    }

    public static int getAvailableInFlightPermits() {
        return inFlightLimiter.availablePermits();
    }

    public static int getRequestTimeoutMillis() {
        return REQUEST_TIMEOUT_MILLIS;
    }

    /** Active HTTP operations that have acquired a permit but not yet completed. */
    public static int getInFlightRequestCount() {
        return MAX_IN_FLIGHT_REQUESTS - inFlightLimiter.availablePermits();
    }

    /**
     * Blocks until all in-flight HTTP operations finish or the timeout elapses.
     * Call before reading final metrics so terminal and CSV totals match.
     */
    public static void waitForInflightDrain(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getInFlightRequestCount() == 0) {
                return;
            }
            Thread.sleep(50);
        }
    }

    private static SslContext createSslContext() {
        try {
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);

            SslContextBuilder builder = SslContextBuilder.forClient()
                    .trustManager(trustManagerFactory);

            try {
                SslContext context = builder.sslProvider(SslProvider.OPENSSL).build();
                System.out.println("[HttpExecutor] TLS provider: OpenSSL (tcnative)");
                return context;
            } catch (Exception opensslError) {
                System.out.println("[HttpExecutor] TLS provider: JDK (OpenSSL unavailable)");
                return builder.sslProvider(SslProvider.JDK).build();
            }

        } catch (SSLException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // =========================================================================
    // Execution API (Public Entry Points)
    // =========================================================================

    public CompletableFuture<RequestMetric> executeAsync(
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore
    ) {
        long overallStartTime = System.nanoTime();

        // Global in-flight cap applies to every HTTP attempt across all virtual users.
        return inFlightLimiter.acquire()
                .thenCompose(ignored -> executeAsyncInternal(
                        userId,
                        personaName,
                        baseUrl,
                        step,
                        variableStore,
                        overallStartTime,
                        0
                ))
                .whenComplete((metric, throwable) -> inFlightLimiter.release());
    }

    private CompletableFuture<RequestMetric> executeAsyncInternal(
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            long overallStartTime,
            int attempt
    ) {
        try {
            String url = buildUrl(baseUrl, step, variableStore);
            URI uri = URI.create(url);

            HttpDebugLog.debug("HTTP REQUEST step=" + step.name + " url=" + url);

            String scheme = uri.getScheme() == null ? "http" : uri.getScheme().toLowerCase();

            if (!"http".equals(scheme) && !"https".equals(scheme)) {
                return CompletableFuture.completedFuture(failureMetric(
                        userId, personaName, step.name, overallStartTime,
                        RequestFailureReason.UNSUPPORTED_SCHEME, "scheme=" + scheme
                ));
            }

            boolean isHttps = "https".equals(scheme);
            String methodName = step.method == null ? "GET" : step.method.toUpperCase();
            HttpMethod httpMethod = HttpMethod.valueOf(methodName);
            boolean needsResponseBody = step.extract != null && !step.extract.isEmpty();
            String body = step.body != null ? getJsonBody(step, variableStore) : "";

            if (connectionMode == ConnectionMode.POOLED) {
                FixedChannelPool pool = getPool(uri, isHttps);
                return toCompletableFuture(pool.acquire())
                        .thenCompose(channel -> sendOnChannel(
                                channel,
                                null,
                                pool,
                                uri,
                                userId,
                                personaName,
                                baseUrl,
                                step,
                                variableStore,
                                httpMethod,
                                body,
                                needsResponseBody,
                                overallStartTime,
                                attempt
                        ))
                        .exceptionally(error -> failureMetric(
                                userId,
                                personaName,
                                step.name,
                                overallStartTime,
                                RequestFailureReason.POOL_ACQUIRE_FAILED,
                                error.getMessage()
                        ));
            }

            return getOrCreateUserChannelAsync(userId, uri, isHttps)
                    .thenCompose(lease -> sendOnChannel(
                            lease.channel,
                            lease,
                            null,
                            uri,
                            userId,
                            personaName,
                            baseUrl,
                            step,
                            variableStore,
                            httpMethod,
                            body,
                            needsResponseBody,
                            overallStartTime,
                            attempt
                    ))
                    .exceptionally(error -> failureMetric(
                            userId,
                            personaName,
                            step.name,
                            overallStartTime,
                            RequestFailureReason.POOL_ACQUIRE_FAILED,
                            error.getMessage()
                    ));

        } catch (Exception e) {
            return CompletableFuture.completedFuture(failureMetric(
                    userId,
                    personaName,
                    step.name,
                    overallStartTime,
                    RequestFailureReason.EXECUTION_ERROR,
                    e.getMessage()
            ));
        }
    }

    // =========================================================================
    // Channel Acquisition & Send Dispatch
    // =========================================================================

    private CompletableFuture<RequestMetric> sendOnChannel(
            Channel channel,
            UserChannelLease lease,
            FixedChannelPool pool,
            URI uri,
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            HttpMethod httpMethod,
            String body,
            boolean needsResponseBody,
            long overallStartTime,
            int attempt
    ) {
        CompletableFuture<RequestMetric> resultFuture = new CompletableFuture<>();

        Runnable sendWork = () -> runSendOnChannel(
                channel,
                lease,
                pool,
                uri,
                userId,
                personaName,
                baseUrl,
                step,
                variableStore,
                httpMethod,
                body,
                needsResponseBody,
                overallStartTime,
                attempt,
                resultFuture
        );

        if (lease != null) {
            registerCancellationCleanup(resultFuture, lease);
            lease.enqueue(ignored -> sendWork.run());
        } else {
            registerCancellationCleanup(resultFuture, channel, pool);
            if (channel.eventLoop().inEventLoop()) {
                sendWork.run();
            } else {
                channel.eventLoop().execute(sendWork);
            }
        }

        return resultFuture;
    }

    // =========================================================================
    // Cancellation & Cleanup Handlers
    // =========================================================================

    private void registerCancellationCleanup(
            CompletableFuture<RequestMetric> resultFuture,
            UserChannelLease lease
    ) {
        resultFuture.whenComplete((metric, error) -> {
            if (!resultFuture.isCancelled()) {
                return;
            }

            Channel channel = lease.channel;
            PendingRequest pending = channel.attr(PENDING_REQUEST).getAndSet(null);

            if (pending != null) {
                cancelPendingRequest(pending, RequestFailureReason.CANCELLED, "request cancelled");
            }
        });
    }

    private void registerCancellationCleanup(
            CompletableFuture<RequestMetric> resultFuture,
            Channel channel,
            FixedChannelPool pool
    ) {
        resultFuture.whenComplete((metric, error) -> {
            if (!resultFuture.isCancelled()) {
                return;
            }

            PendingRequest pending = channel.attr(PENDING_REQUEST).getAndSet(null);

            if (pending != null) {
                cancelPendingRequest(pending, RequestFailureReason.CANCELLED, "request cancelled");
            } else if (pool != null) {
                releasePooledChannel(pool, channel);
            }
        });
    }

    // =========================================================================
    // Core Send & Retry Logic
    // =========================================================================

    private void runSendOnChannel(
            Channel channel,
            UserChannelLease lease,
            FixedChannelPool pool,
            URI uri,
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            HttpMethod httpMethod,
            String body,
            boolean needsResponseBody,
            long overallStartTime,
            int attempt,
            CompletableFuture<RequestMetric> resultFuture
    ) {
        if (resultFuture.isCancelled()) {
            onRequestComplete(lease, pool, channel, true);
            return;
        }

        boolean sticky = lease != null;
        if (sticky && (!lease.isValid() || !channel.isWritable())) {
            invalidateChannel(lease, pool, channel);

            if (attempt < MAX_WRITE_RETRIES && isIdempotent(httpMethod)) {
                executeAsyncInternal(
                        userId, personaName, baseUrl, step, variableStore, overallStartTime, attempt + 1
                ).whenComplete((metric, throwable) -> {
                    onRequestComplete(lease, pool, channel, false);
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(metric);
                    }
                });
                return;
            }

            completeFailure(
                    resultFuture, userId, personaName, step.name, overallStartTime,
                    RequestFailureReason.CHANNEL_INACTIVE, "channel not writable"
            );
            onRequestComplete(lease, pool, channel, false);
            return;
        }

        if (!sticky && (!channel.isOpen() || !channel.isActive() || !channel.isWritable())) {
            closePooledChannel(pool, channel);

            if (attempt < MAX_WRITE_RETRIES && isIdempotent(httpMethod)) {
                executeAsyncInternal(
                        userId, personaName, baseUrl, step, variableStore, overallStartTime, attempt + 1
                ).whenComplete((metric, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(metric);
                    }
                });
                return;
            }

            completeFailure(
                    resultFuture, userId, personaName, step.name, overallStartTime,
                    RequestFailureReason.CHANNEL_INACTIVE, "channel not writable"
            );
            return;
        }

        PendingRequest pendingRequest = new PendingRequest(
                userId,
                personaName,
                step,
                variableStore,
                needsResponseBody,
                overallStartTime,
                resultFuture,
                channel,
                lease,
                pool
        );

        channel.attr(PENDING_REQUEST).set(pendingRequest);

        pendingRequest.timeoutFuture = channel.eventLoop().schedule(() -> {
            PendingRequest pending = channel.attr(PENDING_REQUEST).getAndSet(null);
            if (pending == null) {
                return;
            }
            cancelPendingRequest(pending, RequestFailureReason.TIMEOUT,
                    "timed out after " + REQUEST_TIMEOUT_MILLIS + " ms");
        }, REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        resultFuture.whenComplete((ignored, throwable) -> {
            if (pendingRequest.timeoutFuture != null) {
                pendingRequest.timeoutFuture.cancel(false);
            }
        });

        FullHttpRequest request = buildNettyRequest(uri, httpMethod, body, step, variableStore);

        if (metricsCollector != null && metricsCollector.isTrackSentRpsEnabled()) {
            metricsCollector.recordRequestSent();
        }

        channel.writeAndFlush(request).addListener(writeFuture -> {
            if (writeFuture.isSuccess()) {
                return;
            }

            Throwable cause = writeFuture.cause();
            PendingRequest pending = channel.attr(PENDING_REQUEST).getAndSet(null);

            if (pending != null && pending.timeoutFuture != null) {
                pending.timeoutFuture.cancel(false);
            }

            invalidateChannel(lease, pool, channel);

            if (attempt < MAX_WRITE_RETRIES
                    && isIdempotent(httpMethod)
                    && isClosedChannelFailure(cause)
                    && !resultFuture.isDone()) {

                executeAsyncInternal(
                        userId, personaName, baseUrl, step, variableStore, overallStartTime, attempt + 1
                ).whenComplete((metric, throwable) -> {
                    onRequestComplete(lease, pool, channel, false);
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(metric);
                    }
                });
                return;
            }

            completeFailure(
                    resultFuture, userId, personaName, step.name, overallStartTime,
                    RequestFailureReason.WRITE_FAILED,
                    cause == null ? "write failed" : cause.getMessage()
            );
            onRequestComplete(lease, pool, channel, false);
        });
    }

    private void onRequestComplete(
            UserChannelLease lease,
            FixedChannelPool pool,
            Channel channel,
            boolean releaseHealthy
    ) {
        if (lease != null) {
            lease.onSendComplete();
        } else if (pool != null) {
            if (releaseHealthy) {
                releasePooledChannel(pool, channel);
            } else {
                closePooledChannel(pool, channel);
            }
        }
    }

    private void invalidateChannel(UserChannelLease lease, FixedChannelPool pool, Channel channel) {
        if (lease != null) {
            invalidateLease(lease);
        } else if (pool != null) {
            closePooledChannel(pool, channel);
        }
    }

    private static void releasePooledChannel(FixedChannelPool pool, Channel channel) {
        if (pool == null || channel == null) {
            return;
        }

        pool.release(channel).addListener(releaseFuture -> {
            if (!releaseFuture.isSuccess()) {
                channel.close();
            }
        });
    }

    private static void closePooledChannel(FixedChannelPool pool, Channel channel) {
        if (channel == null) {
            return;
        }

        try {
            channel.close();
        } catch (Exception ignored) {
        }
    }

    // =========================================================================
    // Request State Management & Timeout
    // =========================================================================

    private void cancelPendingRequest(
            PendingRequest pending,
            RequestFailureReason reason,
            String detail
    ) {
        if (pending.timeoutFuture != null) {
            pending.timeoutFuture.cancel(false);
        }

        invalidateChannel(pending.lease, pending.pool, pending.channel);

        if (!pending.resultFuture.isDone()) {
            pending.resultFuture.complete(failureMetric(
                    pending.userId,
                    pending.personaName,
                    pending.step.name,
                    pending.startTime,
                    reason,
                    detail
            ));
        }

        onRequestComplete(pending.lease, pending.pool, pending.channel, false);
    }

    // =========================================================================
    // Channel Management & Pooling
    // =========================================================================

    private CompletableFuture<UserChannelLease> getOrCreateUserChannelAsync(
            String userId,
            URI uri,
            boolean isHttps
    ) {
        String key = getUserChannelKey(userId, uri, isHttps);

        UserChannelLease cached = userChannelCache.get(key);
        if (cached != null && cached.isValid()) {
            return CompletableFuture.completedFuture(cached);
        }

        if (cached != null) {
            userChannelCache.remove(key, cached);
            releaseUserChannel(cached);
        }

        CompletableFuture<UserChannelLease> leader = new CompletableFuture<>();
        CompletableFuture<UserChannelLease> existing = leaseInFlight.putIfAbsent(key, leader);

        if (existing != null) {
            return existing;
        }

        FixedChannelPool pool = getPool(uri, isHttps);

        toCompletableFuture(pool.acquire())
                .thenApply(channel -> registerLease(key, pool, channel))
                .whenComplete((lease, error) -> {
                    leaseInFlight.remove(key, leader);
                    if (error != null) {
                        leader.completeExceptionally(error);
                    } else {
                        leader.complete(lease);
                    }
                });

        return leader;
    }

    private UserChannelLease registerLease(String key, FixedChannelPool pool, Channel channel) {
        UserChannelLease lease = new UserChannelLease(key, pool, channel);

        channel.closeFuture().addListener(future -> {
            userChannelCache.remove(key, lease);
            releaseUserChannel(lease);
        });

        userChannelCache.put(key, lease);
        return lease;
    }

    private void invalidateLease(UserChannelLease lease) {
        userChannelCache.remove(lease.key, lease);

        try {
            lease.channel.close();
        } catch (Exception ignored) {
        }

        releaseUserChannel(lease);
    }

    private static <T> CompletableFuture<T> toCompletableFuture(
            io.netty.util.concurrent.Future<T> nettyFuture
    ) {
        CompletableFuture<T> result = new CompletableFuture<>();

        nettyFuture.addListener(future -> {
            if (future.isSuccess()) {
                @SuppressWarnings("unchecked")
                T value = (T) future.getNow();
                result.complete(value);
            } else {
                result.completeExceptionally(future.cause());
            }
        });

        return result;
    }

    private String getUserChannelKey(String userId, URI uri, boolean isHttps) {
        String scheme = uri.getScheme() == null ? "http" : uri.getScheme().toLowerCase();
        String host = uri.getHost();
        int port = getPort(uri, isHttps);
        return userId + "|" + scheme + "://" + host + ":" + port;
    }

    private FixedChannelPool getPool(URI uri, boolean isHttps) {
        String scheme = uri.getScheme() == null ? "http" : uri.getScheme().toLowerCase();
        String host = uri.getHost();
        int port = getPort(uri, isHttps);
        String key = scheme + "://" + host + ":" + port;

        return poolCache.computeIfAbsent(key, ignored -> {
            Bootstrap bootstrap = new Bootstrap()
                    .group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.SO_RCVBUF, 64 * 1024)
                    .option(ChannelOption.SO_SNDBUF, 64 * 1024)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                    .remoteAddress(host, port);

            // HTTP/1.1 today. HTTP/2 upgrade (ALPN + multiplexing) is planned for a later release.
            return new FixedChannelPool(
                    bootstrap,
                    new AbstractChannelPoolHandler() {
                        @Override
                        public void channelCreated(Channel channel) {
                            ChannelPipeline pipeline = channel.pipeline();

                            if (isHttps) {
                                SslHandler sslHandler = SSL_CONTEXT.newHandler(
                                        channel.alloc(), host, port
                                );
                                sslHandler.setHandshakeTimeoutMillis(SSL_HANDSHAKE_TIMEOUT_MILLIS);

                                SSLParameters sslParameters = sslHandler.engine().getSSLParameters();
                                sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                                sslHandler.engine().setSSLParameters(sslParameters);

                                pipeline.addLast(sslHandler);
                            }

                            pipeline.addLast(new HttpClientCodec());
                            pipeline.addLast(new HttpObjectAggregator(MAX_RESPONSE_SIZE_BYTES));
                            pipeline.addLast(new PermanentResponseHandler());
                        }
                    },
                    ChannelHealthChecker.ACTIVE,
                    FixedChannelPool.AcquireTimeoutAction.FAIL,
                    ACQUIRE_TIMEOUT_MILLIS,
                    MAX_CONNECTIONS_PER_HOST,
                    MAX_PENDING_ACQUIRES,
                    false
            );
        });
    }

    // =========================================================================
    // Inbound Netty Response Handler
    // =========================================================================

    private class PermanentResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) {
            Channel channel = ctx.channel();
            PendingRequest pending = channel.attr(PENDING_REQUEST).getAndSet(null);

            if (pending == null) {
                return;
            }

            if (pending.timeoutFuture != null) {
                pending.timeoutFuture.cancel(false);
            }

            long responseTime = elapsedMillis(pending.startTime);
            int statusCode = response.status().code();

            HttpDebugLog.debug("HTTP RESPONSE step=" + pending.step.name + " status=" + statusCode);

            if (pending.needsResponseBody) {
                String responseBody = response.content().toString(StandardCharsets.UTF_8);
                offloadExtractionAndComplete(pending, statusCode, responseTime, responseBody);
            } else {
                boolean success = pending.step.expectedStatus == null
                        || statusCode == pending.step.expectedStatus;
                RequestFailureReason reason = success
                        ? RequestFailureReason.NONE
                        : RequestFailureReason.HTTP_STATUS_MISMATCH;
                String detail = success ? null : "expected "
                        + pending.step.expectedStatus + " got " + statusCode;

                completeSuccess(pending, statusCode, responseTime, success, reason, detail);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            HttpDebugLog.warn("[HTTP] Channel exception", cause);
            failPending(ctx.channel(), RequestFailureReason.CHANNEL_CLOSED, cause);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            failPending(ctx.channel(), RequestFailureReason.CHANNEL_CLOSED, null);
            ctx.fireChannelInactive();
        }

        private void failPending(Channel channel, RequestFailureReason reason, Throwable cause) {
            PendingRequest pending = channel.attr(PENDING_REQUEST).getAndSet(null);

            if (pending == null) {
                return;
            }

            if (pending.timeoutFuture != null) {
                pending.timeoutFuture.cancel(false);
            }

            long responseTime = elapsedMillis(pending.startTime);
            invalidateChannel(pending.lease, pending.pool, pending.channel);

            if (!pending.resultFuture.isDone()) {
                pending.resultFuture.complete(failureMetric(
                        pending.userId,
                        pending.personaName,
                        pending.step.name,
                        pending.startTime,
                        reason,
                        cause == null ? "channel inactive" : cause.getMessage()
                ));
            }

            onRequestComplete(pending.lease, pending.pool, pending.channel, false);
        }
    }

    // =========================================================================
    // Response Extraction & Metric Completion
    // =========================================================================

    private void offloadExtractionAndComplete(
            PendingRequest pending,
            int statusCode,
            long responseTime,
            String responseBody
    ) {
        extractionExecutor.execute(() -> {
            boolean success = pending.step.expectedStatus == null
                    || statusCode == pending.step.expectedStatus;
            RequestFailureReason failureReason = RequestFailureReason.NONE;
            String errorDetail = null;

            if (!success) {
                failureReason = RequestFailureReason.HTTP_STATUS_MISMATCH;
                errorDetail = "expected " + pending.step.expectedStatus + " got " + statusCode;
            } else {
                try {
                    extractVariables(pending.step, responseBody, pending.variableStore);
                } catch (Exception e) {
                    success = false;
                    failureReason = RequestFailureReason.EXTRACTION_FAILED;
                    errorDetail = e.getMessage();
                    HttpDebugLog.warn("[HTTP] Response extraction failed for step " + pending.step.name, e);
                }
            }

            boolean finalSuccess = success;
            RequestFailureReason finalReason = failureReason;
            String finalDetail = errorDetail;

            pending.channel.eventLoop().execute(() ->
                    completeSuccess(pending, statusCode, responseTime, finalSuccess, finalReason, finalDetail)
            );
        });
    }

    private void completeSuccess(
            PendingRequest pending,
            int statusCode,
            long responseTime,
            boolean success,
            RequestFailureReason failureReason,
            String errorDetail
    ) {
        onRequestComplete(pending.lease, pending.pool, pending.channel, true);

        if (!pending.resultFuture.isDone()) {
            pending.resultFuture.complete(new RequestMetric(
                    pending.userId,
                    pending.personaName,
                    pending.step.name,
                    statusCode,
                    responseTime,
                    success,
                    success ? RequestFailureReason.NONE : failureReason,
                    errorDetail
            ));
        }
    }

    private static class PendingRequest {
        final String userId;
        final String personaName;
        final ApiStep step;
        final VariableStore variableStore;
        final boolean needsResponseBody;
        final long startTime;
        final CompletableFuture<RequestMetric> resultFuture;
        final Channel channel;
        final UserChannelLease lease;
        final FixedChannelPool pool;

        volatile ScheduledFuture<?> timeoutFuture;

        PendingRequest(
                String userId,
                String personaName,
                ApiStep step,
                VariableStore variableStore,
                boolean needsResponseBody,
                long startTime,
                CompletableFuture<RequestMetric> resultFuture,
                Channel channel,
                UserChannelLease lease,
                FixedChannelPool pool
        ) {
            this.userId = userId;
            this.personaName = personaName;
            this.step = step;
            this.variableStore = variableStore;
            this.needsResponseBody = needsResponseBody;
            this.startTime = startTime;
            this.resultFuture = resultFuture;
            this.channel = channel;
            this.lease = lease;
            this.pool = pool;
        }
    }

    // =========================================================================
    // Request Building & Utilities
    // =========================================================================

    private FullHttpRequest buildNettyRequest(
            URI uri,
            HttpMethod method,
            String body,
            ApiStep step,
            VariableStore variableStore
    ) {
        String requestPath = getRequestPath(uri);
        byte[] bodyBytes = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);

        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                method,
                requestPath,
                Unpooled.wrappedBuffer(bodyBytes)
        );

        HttpHeaders headers = request.headers();
        headers.set(HttpHeaderNames.HOST, getHostHeader(uri));
        headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        headers.set(HttpHeaderNames.ACCEPT, "*/*");

        if (bodyBytes.length > 0) {
            headers.set(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);
            addDefaultContentType(headers, step);
        } else {
            headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
        }

        addHeaders(headers, step, variableStore);
        return request;
    }

    private String getRequestPath(URI uri) {
        String path = uri.getRawPath();
        if (path == null || path.isBlank()) {
            path = "/";
        }

        String query = uri.getRawQuery();
        if (query != null && !query.isBlank()) {
            return path + "?" + query;
        }
        return path;
    }

    private String getHostHeader(URI uri) {
        int port = uri.getPort();
        boolean isHttps = "https".equalsIgnoreCase(uri.getScheme());
        int defaultPort = isHttps ? 443 : 80;

        if (port == -1 || port == defaultPort) {
            return uri.getHost();
        }
        return uri.getHost() + ":" + port;
    }

    private int getPort(URI uri, boolean isHttps) {
        if (uri.getPort() != -1) {
            return uri.getPort();
        }
        return isHttps ? 443 : 80;
    }

    private String buildUrl(String baseUrl, ApiStep step, VariableStore variableStore) {
        String resolvedUrl;

        if (step.url != null && !step.url.isBlank()) {
            resolvedUrl = VARIABLE_RESOLVER.resolve(step.url, variableStore);
        } else {
            String resolvedPath = VARIABLE_RESOLVER.resolve(step.path, variableStore);
            resolvedUrl = baseUrl + resolvedPath;
        }

        if (step.queryParams == null || step.queryParams.isEmpty()) {
            return resolvedUrl;
        }

        StringBuilder url = new StringBuilder(resolvedUrl);

        if (!resolvedUrl.contains("?")) {
            url.append("?");
        } else if (!resolvedUrl.endsWith("&") && !resolvedUrl.endsWith("?")) {
            url.append("&");
        }

        for (Map.Entry<String, String> entry : step.queryParams.entrySet()) {
            String resolvedValue = VARIABLE_RESOLVER.resolve(entry.getValue(), variableStore);
            url.append(entry.getKey()).append("=").append(resolvedValue).append("&");
        }

        url.deleteCharAt(url.length() - 1);
        return url.toString();
    }

    private String getJsonBody(ApiStep step, VariableStore variableStore) throws Exception {
        if (step.body == null) {
            return "";
        }
        if (step.cachedJsonBody != null) {
            return step.cachedJsonBody;
        }
        String jsonBody = objectMapper.writeValueAsString(step.body);
        return VARIABLE_RESOLVER.resolve(jsonBody, variableStore);
    }

    private void addDefaultContentType(HttpHeaders headers, ApiStep step) {
        boolean hasContentType = false;

        if (step.headers != null) {
            for (String key : step.headers.keySet()) {
                if ("Content-Type".equalsIgnoreCase(key)) {
                    hasContentType = true;
                    break;
                }
            }
        }

        if (!hasContentType) {
            headers.set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        }
    }

    private void addHeaders(HttpHeaders headers, ApiStep step, VariableStore variableStore) {
        if (step.headers == null) {
            return;
        }

        for (Map.Entry<String, String> header : step.headers.entrySet()) {
            String resolvedValue = VARIABLE_RESOLVER.resolve(header.getValue(), variableStore);
            headers.set(header.getKey(), resolvedValue);
        }
    }

    private void extractVariables(ApiStep step, String responseBody, VariableStore variableStore)
            throws Exception {

        if (step.extract == null || step.extract.isEmpty()) {
            return;
        }

        for (Map.Entry<String, String> entry : step.extract.entrySet()) {
            String value = RESPONSE_EXTRACTOR.extract(responseBody, entry.getValue());
            HttpDebugLog.debug("EXTRACT " + entry.getKey() + "=" + value);

            if (value != null) {
                variableStore.put(entry.getKey(), value);
            }
        }
    }

    private boolean isIdempotent(HttpMethod method) {
        return HttpMethod.GET.equals(method)
                || HttpMethod.HEAD.equals(method)
                || HttpMethod.OPTIONS.equals(method)
                || HttpMethod.DELETE.equals(method);
    }

    private boolean isClosedChannelFailure(Throwable cause) {
        if (cause == null) {
            return false;
        }

        return cause instanceof ClosedChannelException
                || cause.getClass().getName().contains("ClosedChannelException")
                || cause.getClass().getName().contains("StacklessClosedChannelException");
    }

    private RequestMetric failureMetric(
            String userId,
            String personaName,
            String stepName,
            long startTime,
            RequestFailureReason reason,
            String detail
    ) {
        return new RequestMetric(
                userId,
                personaName,
                stepName,
                0,
                elapsedMillis(startTime),
                false,
                reason,
                detail
        );
    }

    private void completeFailure(
            CompletableFuture<RequestMetric> resultFuture,
            String userId,
            String personaName,
            String stepName,
            long startTime,
            RequestFailureReason reason,
            String detail
    ) {
        if (!resultFuture.isDone()) {
            resultFuture.complete(failureMetric(userId, personaName, stepName, startTime, reason, detail));
        }
    }

    private long elapsedMillis(long startTime) {
        return (System.nanoTime() - startTime) / 1_000_000;
    }

    private static void releaseUserChannel(UserChannelLease lease) {
        if (lease == null || lease.pool == null || lease.channel == null) {
            return;
        }

        if (!lease.released.compareAndSet(false, true)) {
            return;
        }

        try {
            lease.pool.release(lease.channel).addListener(releaseFuture -> {
                if (!releaseFuture.isSuccess()) {
                    HttpDebugLog.warn("[HTTP] User channel release failed", releaseFuture.cause());
                    lease.channel.close();
                }
            });
        } catch (Exception e) {
            HttpDebugLog.warn("[HTTP] User channel release exception", e);
            try {
                lease.channel.close();
            } catch (Exception ignored) {
            }
        }
    }

    public static void shutdown() {
        extractionExecutor.shutdown();

        for (UserChannelLease lease : userChannelCache.values()) {
            try {
                releaseUserChannel(lease);
            } catch (Exception ignored) {
            }
        }

        userChannelCache.clear();
        leaseInFlight.clear();

        for (FixedChannelPool pool : poolCache.values()) {
            try {
                pool.close();
            } catch (Exception ignored) {
            }
        }

        eventLoopGroup.shutdownGracefully();
    }
}
