package redis.storage

import io.netty.channel.EventLoopGroup
import redis.config.RedisConfig
import java.util.concurrent.TimeUnit

class KeyExpirationScheduler(
    private val keyOps: KeyOperations,
    private val config: RedisConfig = RedisConfig(),
) {
    fun start(eventLoopGroup: EventLoopGroup) {
        eventLoopGroup.next().scheduleAtFixedRate(
            { cleanupExpiredKeys() },
            config.cleanupIntervalMs,
            config.cleanupIntervalMs,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun cleanupExpiredKeys() {
        repeat(config.maxCleanupIterations) {
            val needsMoreCleanup = keyOps.cleanupExpiredKeys()
            if (!needsMoreCleanup) return
        }
    }
}
