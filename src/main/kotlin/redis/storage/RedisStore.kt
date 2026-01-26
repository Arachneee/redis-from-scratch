package redis.storage

class RedisStore(
    val clock: Clock = SystemClock,
) {
    internal val data = HashMap<String, Any>()
    internal val expirationTimes = HashMap<String, Long>()

    fun isExpired(key: String): Boolean {
        val expireAt = expirationTimes[key] ?: return false
        return expireAt <= clock.currentTimeMillis()
    }

    fun removeKey(key: String) {
        data.remove(key)
        expirationTimes.remove(key)
    }

    fun setExpiration(key: String, ttlMillis: Long) {
        expirationTimes[key] = ttlMillis + clock.currentTimeMillis()
    }

    fun setExpirationAt(key: String, expirationMillis: Long) {
        expirationTimes[key] = expirationMillis
    }

    fun containsKey(key: String): Boolean = data.containsKey(key)

    fun size(): Long = data.size.toLong()

    fun flushAll() {
        data.clear()
        expirationTimes.clear()
    }

    fun cleanupExpiredKeysForIteration() {
        val expiredKeys =
            expirationTimes.entries
                .filter { it.value <= clock.currentTimeMillis() }
                .map { it.key }
        expiredKeys.forEach { removeKey(it) }
    }

    companion object {
        const val KEY_NOT_EXISTS = -2L
        const val NO_TTL = -1L
    }
}
