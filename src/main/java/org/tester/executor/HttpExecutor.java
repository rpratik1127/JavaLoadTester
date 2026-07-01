package org.tester.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.tester.config.TestConstants;
import org.tester.control.AsyncSemaphore;
import org.tester.metrics.HttpConnectionMetrics;
import org.tester.metrics.MetricsCollector;
import org.tester.metrics.RequestFailureReason;
import org.tester.metrics.RequestMetric;
import org.tester.metrics.RequestPhaseTimings;
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
import java.util.ArrayDeque;
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
    /** Mutable before first {@link #getPool}; sized per run in request mode via Little's law. */
    private static volatile int maxConnectionsPerHost = TestConstants.MAX_POOL_CONNECTIONS;
    private static final int MAX_PIPELINE_PER_CHANNEL = TestConstants.MAX_PIPELINE_PER_CHANNEL;
    private static final int MAX_PENDING_ACQUIRES = 50_000;
    private static final int MAX_IN_FLIGHT_REQUESTS = 50_000;

    // --- Timeouts (milliseconds) ---
    private static final int ACQUIRE_TIMEOUT_MILLIS = 90_000;
    private static final int CONNECT_TIMEOUT_MILLIS = 20_000;
    private static final int SSL_HANDSHAKE_TIMEOUT_MILLIS = 30_000;
    /** Requests exceeding this duration fail with {@link RequestFailureReason#TIMEOUT}. */
    private static final int REQUEST_TIMEOUT_MILLIS = 60_000;

    /** Tuned for typical JSON API payloads; oversized bodies fail fast at the aggregator. */
    private static final int MAX_RESPONSE_SIZE_BYTES = 64 * 1024;

    private static final int MAX_WRITE_RETRIES = 1;

    /** Limits concurrent HTTP operations across all virtual users. */
    private static final AsyncSemaphore inFlightLimiter =
            new AsyncSemaphore(MAX_IN_FLIGHT_REQUESTS);

    private static final AttributeKey<ChannelFlightState> CHANNEL_FLIGHT =
            AttributeKey.valueOf("channelFlight");

    private static final AttributeKey<Boolean> FLUSH_SCHEDULED =
            AttributeKey.valueOf("flushScheduled");

    private static final int EVENT_LOOP_THREADS =
            Math.max(8, Runtime.getRuntime().availableProcessors() * 2);

    private static final EventLoopGroup eventLoopGroup =
            new NioEventLoopGroup(EVENT_LOOP_THREADS);

    static {
        System.out.printf(
                "[HttpExecutor] Netty event loops: %d, pool: %d conn/host, pipeline: %d/conn%n",
                EVENT_LOOP_THREADS,
                maxConnectionsPerHost,
                MAX_PIPELINE_PER_CHANNEL
        );
    }

    private static final int WARMUP_BATCH_SIZE = 200;

    // =========================================================================
    // Core Executor Configuration & State
    // =========================================================================

    private static volatile ConnectionMode connectionMode = ConnectionMode.POOLED;

    /**
     * When true in {@link ConnectionMode#POOLED}, each virtual user keeps one pooled channel
     * for the whole run instead of acquire/release per request.
     */
    private static volatile boolean dedicatedUserChannels = true;

    /** Status checks and extraction only — lightweight completions stay on the event loop. */
    private static final ExecutorService RESPONSE_PROCESSING_EXECUTOR =
            Executors.newVirtualThreadPerTaskExecutor();

    static {
        System.out.println("[HttpExecutor] Response processing: virtual threads (off event loop)");
    }

    private static final ConcurrentHashMap<String, FixedChannelPool> poolCache =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, UserChannelLease> userChannelCache =
            new ConcurrentHashMap<>();

    /** Prevents duplicate lease creation for the same user+host key. */
    private static final ConcurrentHashMap<String, CompletableFuture<UserChannelLease>> leaseInFlight =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, DedicatedChannel> dedicatedChannelCache =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, CompletableFuture<DedicatedChannel>> dedicatedInFlight =
            new ConcurrentHashMap<>();

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final SslContext SSL_CONTEXT = createSslContext();

    private static final VariableResolver VARIABLE_RESOLVER = new VariableResolver();
    private static final ResponseExtractor RESPONSE_EXTRACTOR = new ResponseExtractor();

    private final MetricsCollector metricsCollector;

    // =========================================================================
    // Initialization & Utility Accessors
    // =========================================================================

    /** Creates an executor without send-time RPS tracking. */
    public HttpExecutor() {
        this.metricsCollector = null;
    }

    /** Creates an executor that records send-time RPS through the metrics collector. */
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

    public static void setDedicatedUserChannels(boolean enabled) {
        dedicatedUserChannels = enabled;
        if (enabled) {
            System.out.println(
                    "[HttpExecutor] Dedicated per-user pooled channels enabled (no per-request acquire)"
            );
        }
    }

    public static boolean isDedicatedUserChannels() {
        return dedicatedUserChannels;
    }

    /**
     * Sets the per-host connection pool cap. Must be called before any pool is created
     * (i.e. before the first HTTP request or warmup for that run).
     */
    public static void setMaxConnectionsPerHost(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("max connections must be positive");
        }
        if (!poolCache.isEmpty()) {
            throw new IllegalStateException("Cannot change pool size after pools are created");
        }
        maxConnectionsPerHost = max;
    }

    public static int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
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

    public static long getActiveChannelCount() {
        return HttpConnectionMetrics.getActiveChannels();
    }

    public static double getConnectionReuseRate() {
        return HttpConnectionMetrics.getConnectionReuseRate();
    }

    public static double getRequestsPerConnection() {
        return HttpConnectionMetrics.getRequestsPerConnection();
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

    public static CompletableFuture<Void> warmPool(String baseUrl, int connections) {
        return new HttpExecutor(null).warmPoolInternal(baseUrl, connections);
    }

    private CompletableFuture<Void> warmPoolInternal(String baseUrl, int connections) {
        if (connections <= 0) {
            return CompletableFuture.completedFuture(null);
        }

        URI uri = URI.create(baseUrl);
        String scheme = uri.getScheme() == null ? "http" : uri.getScheme().toLowerCase();
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            return CompletableFuture.completedFuture(null);
        }

        boolean isHttps = "https".equals(scheme);
        FixedChannelPool pool = getPool(uri, isHttps);
        int toWarm = Math.min(connections, maxConnectionsPerHost);

        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int warmed = 0; warmed < toWarm; warmed += WARMUP_BATCH_SIZE) {
            int batch = Math.min(WARMUP_BATCH_SIZE, toWarm - warmed);
            chain = chain.thenCompose(ignored -> warmPoolBatch(pool, batch));
        }
        return chain;
    }

    private static CompletableFuture<Void> warmPoolBatch(FixedChannelPool pool, int count) {
        CompletableFuture<?>[] warmers = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            warmers[i] = toCompletableFuture(pool.acquire())
                    .thenAccept(channel -> releasePooledChannel(pool, channel))
                    .exceptionally(error -> null);
        }
        return CompletableFuture.allOf(warmers);
    }

    public static void releaseDedicatedChannel(String userId, String baseUrl) {
        if (userId == null || baseUrl == null || baseUrl.isBlank()) {
            return;
        }

        URI uri = URI.create(baseUrl);
        boolean isHttps = "https".equalsIgnoreCase(
                uri.getScheme() == null ? "http" : uri.getScheme()
        );
        String key = getUserChannelKey(userId, uri, isHttps);
        DedicatedChannel dedicated = dedicatedChannelCache.remove(key);
        if (dedicated != null) {
            releaseDedicatedChannel(dedicated);
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
            } catch (Throwable opensslError) {
                System.out.println("[HttpExecutor] TLS provider: JDK (OpenSSL unavailable: "
                        + opensslError.getMessage() + ")");
                return builder.sslProvider(SslProvider.JDK).build();
            }

        } catch (SSLException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // =========================================================================
    // Execution API (Public Entry Points)
    // =========================================================================

    /** Asynchronously executes one HTTP step and returns timing-enriched metrics. */
    public CompletableFuture<RequestMetric> executeAsync(
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore
    ) {
        return beginSend(userId, personaName, baseUrl, step, variableStore, 0);
    }

    /**
     * Resolves a channel first, then acquires an in-flight permit only for the active
     * send/response window. Holding the permit during pool acquire was a major source
     * of client-side queuing under load.
     */
    private CompletableFuture<RequestMetric> beginSend(
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            int attempt
    ) {
        try {
            RequestPhaseTimings timings = new RequestPhaseTimings();
            timings.requestCreatedNanos = System.nanoTime();

            ResolvedSend resolved = resolveSend(
                    userId, personaName, baseUrl, step, variableStore, timings);

            if (resolved.error != null) {
                return CompletableFuture.completedFuture(resolved.error);
            }

            return resolved.channelFuture.thenCompose(binding ->
                    inFlightLimiter.acquire().thenCompose(ignored -> {
                        CompletableFuture<RequestMetric> result = sendOnChannel(
                                binding.channel,
                                binding.lease,
                                binding.pool,
                                resolved.request,
                                userId,
                                personaName,
                                baseUrl,
                                step,
                                variableStore,
                                resolved.needsResponseBody,
                                binding.retainInPool,
                                resolved.timings,
                                attempt
                        );
                        result.whenComplete((metric, throwable) -> inFlightLimiter.release());
                        return result;
                    })
            ).exceptionally(error -> withTimings(failureMetric(
                    userId,
                    personaName,
                    step.name,
                    System.nanoTime(),
                    RequestFailureReason.POOL_ACQUIRE_FAILED,
                    error.getMessage()
            ), timings));

        } catch (Exception e) {
            RequestPhaseTimings timings = new RequestPhaseTimings();
            timings.requestCreatedNanos = System.nanoTime();
            return CompletableFuture.completedFuture(withTimings(failureMetric(
                    userId,
                    personaName,
                    step.name,
                    System.nanoTime(),
                    RequestFailureReason.EXECUTION_ERROR,
                    e.getMessage()
            ), timings));
        }
    }

    /**
     * Retries channel resolution and send while reusing the caller's in-flight permit.
     */
    private CompletableFuture<RequestMetric> retrySend(
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            int attempt
    ) {
        try {
            RequestPhaseTimings timings = new RequestPhaseTimings();
            timings.requestCreatedNanos = System.nanoTime();
            ResolvedSend resolved = resolveSend(
                    userId, personaName, baseUrl, step, variableStore, timings);
            if (resolved.error != null) {
                return CompletableFuture.completedFuture(resolved.error);
            }
            return resolved.channelFuture.thenCompose(binding -> sendOnChannel(
                    binding.channel,
                    binding.lease,
                    binding.pool,
                    resolved.request,
                    userId,
                    personaName,
                    baseUrl,
                    step,
                    variableStore,
                    resolved.needsResponseBody,
                    binding.retainInPool,
                    resolved.timings,
                    attempt
            ));
        } catch (Exception e) {
            RequestPhaseTimings timings = new RequestPhaseTimings();
            timings.requestCreatedNanos = System.nanoTime();
            return CompletableFuture.completedFuture(withTimings(failureMetric(
                    userId,
                    personaName,
                    step.name,
                    System.nanoTime(),
                    RequestFailureReason.EXECUTION_ERROR,
                    e.getMessage()
            ), timings));
        }
    }

    private ResolvedSend resolveSend(
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            RequestPhaseTimings timings
    ) throws Exception {
        String url = buildUrl(baseUrl, step, variableStore);
        URI uri = URI.create(url);

        HttpDebugLog.debug("HTTP REQUEST step=" + step.name + " url=" + url);

        String scheme = uri.getScheme() == null ? "http" : uri.getScheme().toLowerCase();
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            timings.requestCreatedNanos = timings.requestCreatedNanos > 0
                    ? timings.requestCreatedNanos : System.nanoTime();
            return ResolvedSend.error(withTimings(failureMetric(
                    userId, personaName, step.name, System.nanoTime(),
                    RequestFailureReason.UNSUPPORTED_SCHEME, "scheme=" + scheme
            ), timings));
        }

        boolean isHttps = "https".equals(scheme);
        String methodName = step.method == null ? "GET" : step.method.toUpperCase();
        HttpMethod httpMethod = HttpMethod.valueOf(methodName);
        boolean needsResponseBody = step.extract != null && !step.extract.isEmpty();
        FullHttpRequest request;
        if (step.cachedRequestTemplate != null) {
            request = duplicateRequestTemplate(step);
        } else {
            String body = step.body != null ? getJsonBody(step, variableStore) : "";
            request = buildNettyRequest(uri, httpMethod, body, step, variableStore);
        }

        CompletableFuture<ChannelBinding> channelFuture;
        timings.connectionAcquireStartNanos = System.nanoTime();
        if (connectionMode == ConnectionMode.POOLED) {
            FixedChannelPool pool = getPool(uri, isHttps);
            if (dedicatedUserChannels) {
                channelFuture = getOrCreateDedicatedChannelAsync(userId, uri, isHttps, timings)
                        .thenApply(dedicated -> new ChannelBinding(
                                dedicated.channel, null, dedicated.pool, true));
            } else {
                channelFuture = toCompletableFuture(pool.acquire())
                        .thenApply(channel -> {
                            timings.connectionAcquiredNanos = System.nanoTime();
                            return new ChannelBinding(channel, null, pool, false);
                        });
            }
        } else {
            channelFuture = getOrCreateUserChannelAsync(userId, uri, isHttps, timings)
                    .thenApply(lease -> {
                        timings.connectionAcquiredNanos = System.nanoTime();
                        return new ChannelBinding(lease.channel, lease, null, true);
                    });
        }

        return new ResolvedSend(request, needsResponseBody, channelFuture, timings, null);
    }

    private static FullHttpRequest duplicateRequestTemplate(ApiStep step) {
        FullHttpRequest template = step.cachedRequestTemplate;
        FullHttpRequest copy = new DefaultFullHttpRequest(
                template.protocolVersion(),
                template.method(),
                template.uri(),
                template.content().retainedDuplicate()
        );
        copy.headers().set(template.headers());
        return copy;
    }

    private record ChannelBinding(
            Channel channel,
            UserChannelLease lease,
            FixedChannelPool pool,
            boolean retainInPool
    ) {
    }

    private record ResolvedSend(
            FullHttpRequest request,
            boolean needsResponseBody,
            CompletableFuture<ChannelBinding> channelFuture,
            RequestPhaseTimings timings,
            RequestMetric error
    ) {
        static ResolvedSend error(RequestMetric metric) {
            return new ResolvedSend(null, false, null, metric.phaseTimings, metric);
        }
    }

    // =========================================================================
    // Channel Acquisition & Send Dispatch
    // =========================================================================

    private CompletableFuture<RequestMetric> sendOnChannel(
            Channel channel,
            UserChannelLease lease,
            FixedChannelPool pool,
            FullHttpRequest request,
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            boolean needsResponseBody,
            boolean retainInPool,
            RequestPhaseTimings timings,
            int attempt
    ) {
        CompletableFuture<RequestMetric> resultFuture = new CompletableFuture<>();

        Runnable sendWork = () -> runSendOnChannel(
                channel,
                lease,
                pool,
                request,
                userId,
                personaName,
                baseUrl,
                step,
                variableStore,
                needsResponseBody,
                retainInPool,
                timings,
                attempt,
                resultFuture
        );

        if (lease != null) {
            lease.enqueue(ignored -> sendWork.run());
        } else {
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
            UserChannelLease lease,
            PendingRequest boundPending
    ) {
        resultFuture.whenComplete((metric, error) -> {
            if (!resultFuture.isCancelled()) {
                return;
            }

            Channel channel = lease.channel;
            removeInflight(channel, pending -> {
                if (boundPending != null && pending != boundPending) {
                    return false;
                }
                if (boundPending == null && pending.resultFuture != resultFuture) {
                    return false;
                }
                cancelPendingRequest(pending, RequestFailureReason.CANCELLED, "request cancelled");
                return true;
            });
        });
    }

    private void registerCancellationCleanup(
            CompletableFuture<RequestMetric> resultFuture,
            PendingRequest boundPending,
            Channel channel,
            FixedChannelPool pool
    ) {
        resultFuture.whenComplete((metric, error) -> {
            if (!resultFuture.isCancelled()) {
                return;
            }

            boolean removed = removeInflight(channel, pending -> {
                if (boundPending != null && pending != boundPending) {
                    return false;
                }
                if (boundPending == null && pending.resultFuture != resultFuture) {
                    return false;
                }
                cancelPendingRequest(pending, RequestFailureReason.CANCELLED, "request cancelled");
                return true;
            });
            if (!removed && pool != null) {
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
            FullHttpRequest request,
            String userId,
            String personaName,
            String baseUrl,
            ApiStep step,
            VariableStore variableStore,
            boolean needsResponseBody,
            boolean retainInPool,
            RequestPhaseTimings timings,
            int attempt,
            CompletableFuture<RequestMetric> resultFuture
    ) {
        if (resultFuture.isCancelled()) {
            onRequestComplete(lease, pool, channel, true, retainInPool);
            return;
        }

        ChannelFlightState flight = flightState(channel);
        if (flight.inflight.size() >= MAX_PIPELINE_PER_CHANNEL) {
            // Pipeline full: defer until a slot opens. Capture future + context so channel
            // death can fail the permit-holding future without executing the runnable.
            long deferStartTime = timings.beforeWriteNanos > 0
                    ? timings.beforeWriteNanos
                    : (timings.requestCreatedNanos > 0 ? timings.requestCreatedNanos : System.nanoTime());
            flight.deferredSends.addLast(new DeferredSend(
                    () -> runSendOnChannel(
                            channel, lease, pool, request, userId, personaName, baseUrl, step,
                            variableStore, needsResponseBody, retainInPool, timings, attempt, resultFuture
                    ),
                    resultFuture,
                    lease,
                    pool,
                    channel,
                    retainInPool,
                    userId,
                    personaName,
                    step.name,
                    deferStartTime,
                    timings
            ));
            return;
        }

        HttpMethod httpMethod = request.method();

        boolean sticky = lease != null;
        boolean dedicated = retainInPool && lease == null;

        if (!channel.isOpen() || !channel.isActive()) {
            if (sticky) {
                invalidateChannel(lease, pool, channel);
            } else if (pool != null) {
                closePooledChannel(pool, channel);
            }

            if (attempt < MAX_WRITE_RETRIES && isIdempotent(httpMethod)) {
                retrySend(
                        userId, personaName, baseUrl, step, variableStore, attempt + 1
                ).whenComplete((metric, throwable) -> {
                    onRequestComplete(lease, pool, channel, false, retainInPool);
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(metric);
                    }
                });
                return;
            }

            completeFailure(
                    resultFuture, userId, personaName, step.name, System.nanoTime(),
                    RequestFailureReason.CHANNEL_INACTIVE, "channel inactive"
            );
            onRequestComplete(lease, pool, channel, false, retainInPool);
            return;
        }

        if (sticky && !lease.isValid()) {
            invalidateChannel(lease, pool, channel);

            if (attempt < MAX_WRITE_RETRIES && isIdempotent(httpMethod)) {
                retrySend(
                        userId, personaName, baseUrl, step, variableStore, attempt + 1
                ).whenComplete((metric, throwable) -> {
                    onRequestComplete(lease, pool, channel, false, retainInPool);
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(metric);
                    }
                });
                return;
            }

            completeFailure(
                    resultFuture, userId, personaName, step.name, System.nanoTime(),
                    RequestFailureReason.CHANNEL_INACTIVE, "sticky channel invalid"
            );
            onRequestComplete(lease, pool, channel, false, retainInPool);
            return;
        }

        // Per-request pooled mode only — do not gate dedicated keep-alive channels on isWritable();
        // Netty queues writes until the water mark drains (required for POST throughput).
        if (!sticky && !dedicated && !channel.isWritable()) {
            closePooledChannel(pool, channel);

            if (attempt < MAX_WRITE_RETRIES && isIdempotent(httpMethod)) {
                retrySend(
                        userId, personaName, baseUrl, step, variableStore, attempt + 1
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
                    resultFuture, userId, personaName, step.name, System.nanoTime(),
                    RequestFailureReason.CHANNEL_INACTIVE, "channel not writable"
            );
            return;
        }

        timings.beforeWriteNanos = System.nanoTime();
        long sendStartTime = timings.beforeWriteNanos;

        PendingRequest pendingRequest = new PendingRequest(
                userId,
                personaName,
                step,
                variableStore,
                needsResponseBody,
                sendStartTime,
                resultFuture,
                channel,
                lease,
                pool,
                retainInPool,
                timings
        );

        enqueueInflight(channel, pendingRequest);

        if (lease != null) {
            registerCancellationCleanup(resultFuture, lease, pendingRequest);
        } else {
            registerCancellationCleanup(resultFuture, pendingRequest, channel, pool);
        }

        if (metricsCollector != null && metricsCollector.isTrackSentRpsEnabled()) {
            metricsCollector.recordRequestSent();
        }

        channel.write(request).addListener(writeFuture -> {
            if (writeFuture.isSuccess()) {
                timings.writeCompletedNanos = System.nanoTime();
                HttpConnectionMetrics.recordRequestOnConnection();
                scheduleFlush(channel);
                return;
            }

            Throwable cause = writeFuture.cause();
            dequeueInflight(channel, pendingRequest);
            drainDeferredSends(channel);

            invalidateChannel(lease, pool, channel);

            if (attempt < MAX_WRITE_RETRIES
                    && isIdempotent(httpMethod)
                    && isClosedChannelFailure(cause)
                    && !resultFuture.isDone()) {

                retrySend(
                        userId, personaName, baseUrl, step, variableStore, attempt + 1
                ).whenComplete((metric, throwable) -> {
                    onRequestComplete(lease, pool, channel, false, retainInPool);
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(metric);
                    }
                });
                return;
            }

            completeFailure(
                    resultFuture, userId, personaName, step.name, sendStartTime,
                    RequestFailureReason.WRITE_FAILED,
                    cause == null ? "write failed" : cause.getMessage()
            );
            onRequestComplete(lease, pool, channel, false, retainInPool);
        });
        scheduleFlush(channel);
    }

    private static void onRequestComplete(
            UserChannelLease lease,
            FixedChannelPool pool,
            Channel channel,
            boolean releaseHealthy,
            boolean retainInPool
    ) {
        if (lease != null) {
            lease.onSendComplete();
        }
        if (retainInPool) {
            return;
        }
        if (pool != null) {
            if (releaseHealthy) {
                releasePooledChannel(pool, channel);
            } else {
                closePooledChannel(pool, channel);
            }
        }
    }

    private void invalidateChannel(UserChannelLease lease, FixedChannelPool pool, Channel channel) {
        // Fail deferred sends before close — they never entered inflight and hold in-flight permits.
        failDeferredSends(channel, RequestFailureReason.CHANNEL_CLOSED, "channel invalidated");
        if (lease != null) {
            invalidateLease(lease);
        } else if (pool != null) {
            removeDedicatedChannel(channel);
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
        if (!pending.resultFuture.isDone()) {
            pending.timings.fullResponseNanos = System.nanoTime();
            pending.resultFuture.complete(withTimings(failureMetric(
                    pending.userId,
                    pending.personaName,
                    pending.step.name,
                    pending.startTime,
                    reason,
                    detail
            ), pending.timings));
        }

        onRequestComplete(pending.lease, pending.pool, pending.channel, false, pending.retainInPool);
        drainDeferredSends(pending.channel);
    }

    private static void scheduleFlush(Channel channel) {
        if (Boolean.TRUE.equals(channel.attr(FLUSH_SCHEDULED).get())) {
            return;
        }
        channel.attr(FLUSH_SCHEDULED).set(true);
        channel.eventLoop().execute(() -> {
            channel.attr(FLUSH_SCHEDULED).set(false);
            channel.flush();
        });
    }

    private static ChannelFlightState flightState(Channel channel) {
        ChannelFlightState state = channel.attr(CHANNEL_FLIGHT).get();
        if (state == null) {
            state = new ChannelFlightState();
            channel.attr(CHANNEL_FLIGHT).set(state);
        }
        return state;
    }

    private static void enqueueInflight(Channel channel, PendingRequest pending) {
        flightState(channel).inflight.addLast(pending);
    }

    private static boolean dequeueInflight(Channel channel, PendingRequest pending) {
        return flightState(channel).inflight.remove(pending);
    }

    private static PendingRequest peekInflight(Channel channel) {
        ChannelFlightState state = channel.attr(CHANNEL_FLIGHT).get();
        return state == null || state.inflight.isEmpty() ? null : state.inflight.peekFirst();
    }

    private static PendingRequest pollInflight(Channel channel) {
        ChannelFlightState state = channel.attr(CHANNEL_FLIGHT).get();
        return state == null || state.inflight.isEmpty() ? null : state.inflight.pollFirst();
    }

    /** Removes and cancels matching in-flight requests; returns true if any were removed. */
    private static boolean removeInflight(Channel channel, java.util.function.Predicate<PendingRequest> action) {
        ChannelFlightState state = channel.attr(CHANNEL_FLIGHT).get();
        if (state == null || state.inflight.isEmpty()) {
            return false;
        }
        boolean removed = false;
        var iterator = state.inflight.iterator();
        while (iterator.hasNext()) {
            PendingRequest pending = iterator.next();
            if (action.test(pending)) {
                iterator.remove();
                removed = true;
            }
        }
        return removed;
    }

    private static void failAllInflight(Channel channel, RequestFailureReason reason, String detail) {
        ChannelFlightState state = channel.attr(CHANNEL_FLIGHT).get();
        if (state == null) {
            return;
        }
        PendingRequest pending;
        while ((pending = state.inflight.pollFirst()) != null) {
            if (!pending.resultFuture.isDone()) {
                pending.timings.fullResponseNanos = System.nanoTime();
                pending.resultFuture.complete(withTimings(failureMetric(
                        pending.userId,
                        pending.personaName,
                        pending.step.name,
                        pending.startTime,
                        reason,
                        detail
                ), pending.timings));
            }
            onRequestComplete(pending.lease, pending.pool, pending.channel, false, pending.retainInPool);
        }
    }

    /**
     * Fails all pipeline-deferred sends on a dying channel.
     * Triggered from channel-fatal paths before/while the connection closes.
     * Deferred work never reached {@code inflight}; without this, futures and permits leak.
     */
    private static void failDeferredSends(Channel channel, RequestFailureReason reason, String detail) {
        ChannelFlightState state = channel.attr(CHANNEL_FLIGHT).get();
        if (state == null || state.deferredSends.isEmpty()) {
            return;
        }
        DeferredSend deferred;
        while ((deferred = state.deferredSends.pollFirst()) != null) {
            if (!deferred.resultFuture.isDone()) {
                deferred.timings.fullResponseNanos = System.nanoTime();
                deferred.resultFuture.complete(withTimings(failureMetric(
                        deferred.userId,
                        deferred.personaName,
                        deferred.stepName,
                        deferred.startTime,
                        reason,
                        detail
                ), deferred.timings));
            }
            onRequestComplete(
                    deferred.lease, deferred.pool, deferred.channel, false, deferred.retainInPool);
        }
    }

    /**
     * Send waiting for pipeline capacity on this channel.
     * A bare {@link Runnable} cannot complete the caller's future on channel death;
     * the future and lease/pool context must be captured at defer time.
     */
    private static final class DeferredSend {
        final Runnable work;
        final CompletableFuture<RequestMetric> resultFuture;
        final UserChannelLease lease;
        final FixedChannelPool pool;
        final Channel channel;
        final boolean retainInPool;
        final String userId;
        final String personaName;
        final String stepName;
        final long startTime;
        final RequestPhaseTimings timings;

        DeferredSend(
                Runnable work,
                CompletableFuture<RequestMetric> resultFuture,
                UserChannelLease lease,
                FixedChannelPool pool,
                Channel channel,
                boolean retainInPool,
                String userId,
                String personaName,
                String stepName,
                long startTime,
                RequestPhaseTimings timings
        ) {
            this.work = work;
            this.resultFuture = resultFuture;
            this.lease = lease;
            this.pool = pool;
            this.channel = channel;
            this.retainInPool = retainInPool;
            this.userId = userId;
            this.personaName = personaName;
            this.stepName = stepName;
            this.startTime = startTime;
            this.timings = timings;
        }
    }

    private static final class ChannelFlightState {
        final ArrayDeque<PendingRequest> inflight = new ArrayDeque<>();
        final ArrayDeque<DeferredSend> deferredSends = new ArrayDeque<>();
    }

    private static void drainDeferredSends(Channel channel) {
        ChannelFlightState state = channel.attr(CHANNEL_FLIGHT).get();
        if (state == null || state.deferredSends.isEmpty()) {
            return;
        }
        DeferredSend next;
        while (state.inflight.size() < MAX_PIPELINE_PER_CHANNEL
                && (next = state.deferredSends.pollFirst()) != null) {
            next.work.run();
        }
    }

    // =========================================================================
    // Dedicated per-user pooled channels (user mode throughput)
    // =========================================================================

    private static final class DedicatedChannel {
        final String key;
        final FixedChannelPool pool;
        final Channel channel;

        DedicatedChannel(String key, FixedChannelPool pool, Channel channel) {
            this.key = key;
            this.pool = pool;
            this.channel = channel;
        }

        boolean isValid() {
            return channel.isOpen() && channel.isActive();
        }
    }

    private CompletableFuture<DedicatedChannel> getOrCreateDedicatedChannelAsync(
            String userId,
            URI uri,
            boolean isHttps,
            RequestPhaseTimings timings
    ) {
        String key = getUserChannelKey(userId, uri, isHttps);

        DedicatedChannel cached = dedicatedChannelCache.get(key);
        if (cached != null && cached.isValid()) {
            timings.connectionAcquiredNanos = System.nanoTime();
            return CompletableFuture.completedFuture(cached);
        }

        if (cached != null) {
            dedicatedChannelCache.remove(key, cached);
            releaseDedicatedChannel(cached);
        }

        CompletableFuture<DedicatedChannel> leader = new CompletableFuture<>();
        CompletableFuture<DedicatedChannel> existing = dedicatedInFlight.putIfAbsent(key, leader);
        if (existing != null) {
            return existing;
        }

        FixedChannelPool pool = getPool(uri, isHttps);
        toCompletableFuture(pool.acquire())
                .thenApply(channel -> {
                    timings.connectionAcquiredNanos = System.nanoTime();
                    return registerDedicatedChannel(key, pool, channel);
                })
                .whenComplete((dedicated, error) -> {
                    dedicatedInFlight.remove(key, leader);
                    if (error != null) {
                        leader.completeExceptionally(error);
                    } else {
                        leader.complete(dedicated);
                    }
                });

        return leader;
    }

    private DedicatedChannel registerDedicatedChannel(String key, FixedChannelPool pool, Channel channel) {
        DedicatedChannel dedicated = new DedicatedChannel(key, pool, channel);
        channel.closeFuture().addListener(future -> dedicatedChannelCache.remove(key, dedicated));
        dedicatedChannelCache.put(key, dedicated);
        return dedicated;
    }

    private static void removeDedicatedChannel(Channel channel) {
        dedicatedChannelCache.entrySet().removeIf(entry -> {
            if (entry.getValue().channel == channel) {
                releaseDedicatedChannel(entry.getValue());
                return true;
            }
            return false;
        });
    }

    private static void releaseDedicatedChannel(DedicatedChannel dedicated) {
        if (dedicated == null || dedicated.pool == null || dedicated.channel == null) {
            return;
        }
        try {
            dedicated.pool.release(dedicated.channel).addListener(releaseFuture -> {
                if (!releaseFuture.isSuccess()) {
                    dedicated.channel.close();
                }
            });
        } catch (Exception e) {
            HttpDebugLog.warn("[HTTP] Dedicated channel release exception", e);
            try {
                dedicated.channel.close();
            } catch (Exception ignored) {
            }
        }
    }

    // =========================================================================
    // Channel Management & Pooling
    // =========================================================================

    private CompletableFuture<UserChannelLease> getOrCreateUserChannelAsync(
            String userId,
            URI uri,
            boolean isHttps,
            RequestPhaseTimings timings
    ) {
        String key = getUserChannelKey(userId, uri, isHttps);

        UserChannelLease cached = userChannelCache.get(key);
        if (cached != null && cached.isValid()) {
            timings.connectionAcquiredNanos = System.nanoTime();
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
                .thenApply(channel -> {
                    timings.connectionAcquiredNanos = System.nanoTime();
                    return registerLease(key, pool, channel);
                })
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

    private static String getUserChannelKey(String userId, URI uri, boolean isHttps) {
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
                    .option(ChannelOption.AUTO_READ, true)
                    .option(ChannelOption.SO_RCVBUF, 256 * 1024)
                    .option(ChannelOption.SO_SNDBUF, 256 * 1024)
                    .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                            new WriteBufferWaterMark(64 * 1024, 128 * 1024))
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                    .remoteAddress(host, port);

            // HTTP/1.1 with keep-alive pipelining (oha/Postman model).
            return new FixedChannelPool(
                    bootstrap,
                    new AbstractChannelPoolHandler() {
                        @Override
                        public void channelCreated(Channel channel) {
                            channel.attr(CHANNEL_FLIGHT).set(new ChannelFlightState());
                            HttpConnectionMetrics.channelActivated();
                            channel.closeFuture().addListener(f -> HttpConnectionMetrics.channelDeactivated());
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
                            pipeline.addLast(new ReadTimeoutHandler(
                                    REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS
                            ));
                            pipeline.addLast(new ThinInboundResponseHandler());
                        }
                    },
                    ChannelHealthChecker.ACTIVE,
                    FixedChannelPool.AcquireTimeoutAction.FAIL,
                    ACQUIRE_TIMEOUT_MILLIS,
                    maxConnectionsPerHost,
                    MAX_PENDING_ACQUIRES,
                    false
            );
        });
    }

    // =========================================================================
    // Inbound Netty Response Handler (thin on event loop)
    // =========================================================================

    /**
     * Records phase timestamps on the event loop. Lightweight responses (no extraction) complete
     * inline; body extraction is offloaded to virtual threads.
     */
    private final class ThinInboundResponseHandler extends ChannelInboundHandlerAdapter {

        private HttpResponse currentResponse;
        private ByteBuf bodyAccumulator;
        private PendingRequest activePending;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (activePending == null) {
                activePending = peekInflight(ctx.channel());
            }
            PendingRequest pending = activePending;
            if (pending == null) {
                ReferenceCountUtil.release(msg);
                return;
            }

            long now = System.nanoTime();

            if (msg instanceof HttpResponse response) {
                if (pending.timings.firstResponseNanos <= 0) {
                    pending.timings.firstResponseNanos = now;
                }
                pending.timings.lastInboundReadNanos = now;
                currentResponse = response;
                return;
            }

            if (msg instanceof HttpContent content) {
                if (pending.timings.firstBodyChunkNanos <= 0) {
                    pending.timings.firstBodyChunkNanos = now;
                }
                pending.timings.lastInboundReadNanos = now;

                if (pending.needsResponseBody) {
                    int readable = content.content().readableBytes();
                    if (readable > 0) {
                        if (bodyAccumulator == null) {
                            bodyAccumulator = ctx.alloc().buffer(readable);
                        }
                        int newSize = bodyAccumulator.readableBytes() + readable;
                        if (newSize > MAX_RESPONSE_SIZE_BYTES) {
                            content.release();
                            failInboundResponse(
                                    ctx,
                                    pending,
                                    RequestFailureReason.EXTRACTION_FAILED,
                                    "response body exceeds " + MAX_RESPONSE_SIZE_BYTES + " bytes"
                            );
                            return;
                        }
                        bodyAccumulator.writeBytes(content.content());
                    }
                }

                if (content instanceof LastHttpContent) {
                    completeInboundResponse(ctx, pending);
                }
                content.release();
                return;
            }

            ReferenceCountUtil.release(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof ReadTimeoutException) {
                Channel channel = ctx.channel();
                String detail = "read timed out after " + REQUEST_TIMEOUT_MILLIS + " ms";
                activePending = null;
                resetState();
                // HTTP/1.1 pipelining: responses are strictly ordered — one idle timeout
                // means the whole connection is broken; fail all queued work, not just head.
                failAllInflight(channel, RequestFailureReason.TIMEOUT, detail);
                failDeferredSends(channel, RequestFailureReason.TIMEOUT, detail);
                channel.close();
                return;
            }

            HttpDebugLog.warn("[HTTP] Channel exception", cause);
            Channel channel = ctx.channel();
            String detail = cause == null ? "channel exception" : cause.getMessage();
            failAllInflight(channel, RequestFailureReason.CHANNEL_CLOSED, detail);
            failDeferredSends(channel, RequestFailureReason.CHANNEL_CLOSED, detail);
            activePending = null;
            resetState();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            Channel channel = ctx.channel();
            failAllInflight(channel, RequestFailureReason.CHANNEL_CLOSED, "channel inactive");
            // Drain deferred sends that never acquired an inflight slot before the channel died.
            failDeferredSends(channel, RequestFailureReason.CHANNEL_CLOSED, "channel inactive");
            ctx.fireChannelInactive();
        }

        private void completeInboundResponse(ChannelHandlerContext ctx, PendingRequest pending) {
            if (currentResponse == null) {
                return;
            }

            PendingRequest claimed = pollInflight(ctx.channel());
            if (claimed == null || claimed != pending) {
                if (claimed != null) {
                    flightState(ctx.channel()).inflight.addFirst(claimed);
                }
                return;
            }

            activePending = null;

            pending.timings.fullResponseNanos = System.nanoTime();
            int statusCode = currentResponse.status().code();
            long responseTime = elapsedMillis(pending.startTime);

            if (!pending.needsResponseBody) {
                resetState();
                completeSuccessOnEventLoop(pending, statusCode, responseTime, null);
                return;
            }

            byte[] bodyBytes = null;
            if (bodyAccumulator != null && bodyAccumulator.isReadable()) {
                bodyBytes = new byte[bodyAccumulator.readableBytes()];
                bodyAccumulator.readBytes(bodyBytes);
            }

            resetState();
            final byte[] handoffBody = bodyBytes;
            RESPONSE_PROCESSING_EXECUTOR.execute(() ->
                    processResponseHandoff(new ResponseHandoff(pending, statusCode, responseTime, handoffBody))
            );
        }

        private void failInboundResponse(
                ChannelHandlerContext ctx,
                PendingRequest pending,
                RequestFailureReason reason,
                String detail
        ) {
            resetState();
            pollInflight(ctx.channel());
            activePending = null;

            pending.timings.fullResponseNanos = System.nanoTime();
            scheduleResponseFailure(pending, reason, detail, true);
        }

        private void resetState() {
            if (currentResponse != null) {
                ReferenceCountUtil.release(currentResponse);
                currentResponse = null;
            }
            if (bodyAccumulator != null) {
                bodyAccumulator.release();
                bodyAccumulator = null;
            }
        }
    }

    private record ResponseHandoff(
            PendingRequest pending,
            int statusCode,
            long responseTime,
            byte[] bodyBytes
    ) {
    }

    // =========================================================================
    // Response Processing (off event loop when body extraction is required)
    // =========================================================================

    private void completeSuccessOnEventLoop(
            PendingRequest pending,
            int statusCode,
            long responseTime,
            byte[] bodyBytes
    ) {
        boolean success = pending.step.expectedStatus == null
                || statusCode == pending.step.expectedStatus;
        RequestFailureReason failureReason = success
                ? RequestFailureReason.NONE
                : RequestFailureReason.HTTP_STATUS_MISMATCH;
        String errorDetail = success
                ? null
                : "expected " + pending.step.expectedStatus + " got " + statusCode;

        if (success && bodyBytes != null) {
            try {
                String responseBody = new String(bodyBytes, StandardCharsets.UTF_8);
                extractVariables(pending.step, responseBody, pending.variableStore);
            } catch (Exception e) {
                success = false;
                failureReason = RequestFailureReason.EXTRACTION_FAILED;
                errorDetail = e.getMessage();
            }
        }

        RequestMetric metric = new RequestMetric(
                pending.userId,
                pending.personaName,
                pending.step.name,
                statusCode,
                responseTime,
                success,
                success ? RequestFailureReason.NONE : failureReason,
                errorDetail
        );
        metric.phaseTimings = pending.timings;
        finalizeResponseOnEventLoop(pending, metric, true, false);
    }

    private void processResponseHandoff(ResponseHandoff handoff) {
        PendingRequest pending = handoff.pending;
        int statusCode = handoff.statusCode;
        long responseTime = handoff.responseTime;

        boolean success = pending.step.expectedStatus == null
                || statusCode == pending.step.expectedStatus;
        RequestFailureReason failureReason = success
                ? RequestFailureReason.NONE
                : RequestFailureReason.HTTP_STATUS_MISMATCH;
        String errorDetail = success
                ? null
                : "expected " + pending.step.expectedStatus + " got " + statusCode;

        if (success && handoff.bodyBytes != null) {
            try {
                String responseBody = new String(handoff.bodyBytes, StandardCharsets.UTF_8);
                extractVariables(pending.step, responseBody, pending.variableStore);
            } catch (Exception e) {
                success = false;
                failureReason = RequestFailureReason.EXTRACTION_FAILED;
                errorDetail = e.getMessage();
                HttpDebugLog.warn("[HTTP] Response extraction failed for step " + pending.step.name, e);
            }
        }

        HttpDebugLog.debug("HTTP RESPONSE step=" + pending.step.name + " status=" + statusCode);

        boolean finalSuccess = success;
        RequestFailureReason finalReason = failureReason;
        String finalDetail = errorDetail;

        RequestMetric metric = new RequestMetric(
                pending.userId,
                pending.personaName,
                pending.step.name,
                statusCode,
                responseTime,
                finalSuccess,
                finalSuccess ? RequestFailureReason.NONE : finalReason,
                finalDetail
        );
        metric.phaseTimings = pending.timings;

        if (pending.retainInPool) {
            finalizeResponseOnEventLoop(pending, metric, true, false);
        } else {
            pending.channel.eventLoop().execute(() ->
                    finalizeResponseOnEventLoop(pending, metric, true, false)
            );
        }
    }

    private void scheduleResponseFailure(
            PendingRequest pending,
            RequestFailureReason reason,
            String detail,
            boolean channelFailed
    ) {
        if (!pending.needsResponseBody) {
            pending.channel.eventLoop().execute(() -> {
                RequestMetric metric = withTimings(failureMetric(
                        pending.userId,
                        pending.personaName,
                        pending.step.name,
                        pending.startTime,
                        reason,
                        detail
                ), pending.timings);
                finalizeResponseOnEventLoop(pending, metric, !channelFailed, channelFailed);
            });
            return;
        }

        RESPONSE_PROCESSING_EXECUTOR.execute(() -> {
            RequestMetric metric = withTimings(failureMetric(
                    pending.userId,
                    pending.personaName,
                    pending.step.name,
                    pending.startTime,
                    reason,
                    detail
            ), pending.timings);
            pending.channel.eventLoop().execute(() ->
                    finalizeResponseOnEventLoop(pending, metric, !channelFailed, channelFailed)
            );
        });
    }

    private void finalizeResponseOnEventLoop(
            PendingRequest pending,
            RequestMetric metric,
            boolean releaseHealthy,
            boolean shouldInvalidateChannel
    ) {
        if (shouldInvalidateChannel) {
            invalidateChannel(pending.lease, pending.pool, pending.channel);
        }
        onRequestComplete(pending.lease, pending.pool, pending.channel, releaseHealthy, pending.retainInPool);
        if (!pending.resultFuture.isDone()) {
            pending.resultFuture.complete(metric);
        }
        drainDeferredSends(pending.channel);
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
        final boolean retainInPool;
        final RequestPhaseTimings timings;

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
                FixedChannelPool pool,
                boolean retainInPool,
                RequestPhaseTimings timings
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
            this.retainInPool = retainInPool;
            this.timings = timings;
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
        byte[] bodyBytes = resolveBodyBytes(step, body);

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

    private static byte[] resolveBodyBytes(ApiStep step, String body) {
        if (step.cachedBodyBytes != null) {
            return step.cachedBodyBytes;
        }
        if (body == null || body.isEmpty()) {
            return new byte[0];
        }
        return body.getBytes(StandardCharsets.UTF_8);
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

    private static int getPort(URI uri, boolean isHttps) {
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

    private static RequestMetric failureMetric(
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

    private static long elapsedMillis(long startTime) {
        return (System.nanoTime() - startTime) / 1_000_000;
    }

    private static RequestMetric withTimings(RequestMetric metric, RequestPhaseTimings timings) {
        metric.phaseTimings = timings;
        return metric;
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

    /** Releases pooled resources, channels, and the shared Netty event loop. */
    public static void shutdown() {
        RESPONSE_PROCESSING_EXECUTOR.shutdown();

        for (DedicatedChannel dedicated : dedicatedChannelCache.values()) {
            try {
                releaseDedicatedChannel(dedicated);
            } catch (Exception ignored) {
            }
        }
        dedicatedChannelCache.clear();
        dedicatedInFlight.clear();

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
