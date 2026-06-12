package org.tester.executor;

import io.netty.channel.Channel;
import io.netty.channel.pool.FixedChannelPool;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sticky per-user channel lease (userId + host).
 * <p>
 * Lifecycle: one TCP connection is bound to a virtual user for the duration of the test
 * (or until the channel fails). The channel is returned to the host pool only on
 * invalidation or shutdown — not between requests — so session variables and keep-alive
 * behave like Gatling/k6 sticky sessions.
 * <p>
 * Requests on the same lease are serialized through an async queue (HTTP/1.1 safe).
 * HTTP/2 multiplexing may lift this constraint in a future version.
 */
final class UserChannelLease {

    interface SendWork {
        void run(UserChannelLease lease);
    }

    final String key;
    final FixedChannelPool pool;
    final Channel channel;
    final AtomicBoolean released = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<SendWork> sendQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean sending;

    UserChannelLease(String key, FixedChannelPool pool, Channel channel) {
        this.key = key;
        this.pool = pool;
        this.channel = channel;
    }

    boolean isValid() {
        return !released.get()
                && channel.isOpen()
                && channel.isActive();
    }

    void enqueue(SendWork work) {
        sendQueue.add(work);
        channel.eventLoop().execute(this::drainQueue);
    }

    void onSendComplete() {
        sendQueue.poll();
        sending = false;
        drainQueue();
    }

    void failAllQueued(String reason) {
        channel.eventLoop().execute(() -> {
            sending = false;
            sendQueue.clear();
        });
    }

    private void drainQueue() {
        if (sending) {
            return;
        }

        SendWork work = sendQueue.peek();
        if (work == null) {
            return;
        }

        sending = true;
        try {
            work.run(this);
        } catch (Exception error) {
            sending = false;
            sendQueue.poll();
            HttpDebugLog.warn("[HTTP] Queued send failed for lease " + key, error);
            drainQueue();
        }
    }
}
