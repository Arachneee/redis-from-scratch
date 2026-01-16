package redis

import io.netty.channel.EventLoopGroup
import java.util.concurrent.TimeUnit

class KeyExpirationScheduler(
    private val repository: RedisRepository,
    private val cleanupIntervalMs: Long = DEFAULT_CLEANUP_INTERVAL_MS,
    private val maxCleanupIterations: Int = DEFAULT_MAX_CLEANUP_ITERATIONS,
) {
    fun start(eventLoopGroup: EventLoopGroup) {
        eventLoopGroup.next().scheduleAtFixedRate(
            { cleanupExpiredKeys() },
            cleanupIntervalMs,
            cleanupIntervalMs,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun cleanupExpiredKeys() {
        repeat(maxCleanupIterations) {
            val needsMoreCleanup = repository.cleanupExpiredKeys()
            if (!needsMoreCleanup) return
        }
    }

    companion object {
        private const val DEFAULT_CLEANUP_INTERVAL_MS = 100L
        private const val DEFAULT_MAX_CLEANUP_ITERATIONS = 2
    }
}
