package redis.storage

/**
 * Redis 키-값 저장소.
 *
 * 이 클래스는 thread-safe하지 않으며, 단일 스레드(Netty EventLoop) 환경에서만 사용해야 한다.
 * 멀티스레드 환경에서 사용할 경우 외부에서 동기화를 보장해야 한다.
 */
class RedisRepository(
    private val clock: Clock = SystemClock,
) {
    private val store = HashMap<String, ByteArray>()
    private val expirationTimes = HashMap<String, Long>()

    fun cleanupExpiredKeys(): Boolean {
        val keys = expirationTimes.keys.toList()
        if (keys.isEmpty()) return false

        val now = clock.currentTimeMillis()
        val randomKeys = keys.shuffled().take(TTL_EXPIRE_SAMPLES)

        val count =
            randomKeys
                .asSequence()
                .filter { expirationTimes.getOrDefault(it, Long.MAX_VALUE) < now }
                .onEach {
                    store.remove(it)
                    expirationTimes.remove(it)
                }.count()

        return count > TTL_EXPIRE_SAMPLES / 4
    }

    fun get(key: String): ByteArray? {
        if (expirationTimes.getOrDefault(key, Long.MAX_VALUE) > clock.currentTimeMillis()) {
            return store[key]
        } else {
            store.remove(key)
            expirationTimes.remove(key)
            return null
        }
    }

    fun set(
        key: String,
        value: ByteArray,
    ) {
        store[key] = value
        expirationTimes.remove(key)
    }

    fun setWithTtlSeconds(
        key: String,
        value: ByteArray,
        ttlSeconds: Long,
    ) {
        store[key] = value
        setExpiration(key, ttlSeconds * 1000)
    }

    fun setWithTtlMillis(
        key: String,
        value: ByteArray,
        ttlMillis: Long,
    ) {
        store[key] = value
        setExpiration(key, ttlMillis)
    }

    fun delete(key: String): Long {
        val removed = store.remove(key)
        expirationTimes.remove(key)
        return if (removed != null) 1 else 0
    }

    fun delete(keys: Collection<String>): Long = keys.sumOf { delete(it) }

    fun expire(
        key: String,
        seconds: Long,
    ): Long {
        if (!store.containsKey(key)) return 0
        if (seconds <= 0) return delete(key)

        setExpiration(key, seconds * 1000)
        return 1
    }

    fun ttl(key: String): Long {
        val expireAt = expirationTimes[key]
        if (!store.containsKey(key)) return KEY_NOT_EXISTS
        if (expireAt == null) return NO_TTL

        val remainingMillis = expireAt - clock.currentTimeMillis()
        if (remainingMillis <= 0) {
            store.remove(key)
            expirationTimes.remove(key)
            return KEY_NOT_EXISTS
        }
        return (remainingMillis + 999) / 1000
    }

    private fun setExpiration(
        key: String,
        ttlMillis: Long,
    ) {
        expirationTimes[key] = ttlMillis + clock.currentTimeMillis()
    }

    companion object {
        private const val KEY_NOT_EXISTS = -2L
        private const val NO_TTL = -1L
        private const val TTL_EXPIRE_SAMPLES = 20
    }
}
