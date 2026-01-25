package redis.config

data class RedisConfig(
    val port: Int = DEFAULT_PORT,
    val shutdownQuietPeriodMs: Long = DEFAULT_SHUTDOWN_QUIET_PERIOD_MS,
    val shutdownTimeoutMs: Long = DEFAULT_SHUTDOWN_TIMEOUT_MS,
    val cleanupIntervalMs: Long = DEFAULT_CLEANUP_INTERVAL_MS,
    val maxCleanupIterations: Int = DEFAULT_MAX_CLEANUP_ITERATIONS,
    val soBacklog: Int = DEFAULT_SO_BACKLOG,
    val aofFilename: String = DEFAULT_AOF_FILENAME,
) {
    companion object {
        private const val DEFAULT_PORT = 6379
        private const val DEFAULT_SHUTDOWN_QUIET_PERIOD_MS = 100L
        private const val DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000L
        private const val DEFAULT_CLEANUP_INTERVAL_MS = 100L
        private const val DEFAULT_MAX_CLEANUP_ITERATIONS = 2
        private const val DEFAULT_SO_BACKLOG = 1024
        private const val DEFAULT_AOF_FILENAME = "appendonly.aof"
    }
}
