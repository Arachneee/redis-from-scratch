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

    fun exists(keys: Collection<String>): Long =
        keys.count { key ->
            val expireAt = expirationTimes[key]
            if (expireAt != null && expireAt <= clock.currentTimeMillis()) {
                store.remove(key)
                expirationTimes.remove(key)
                false
            } else {
                store.containsKey(key)
            }
        }.toLong()

    fun size(): Long = store.size.toLong()

    fun flushAll() {
        store.clear()
        expirationTimes.clear()
    }

    fun persist(key: String): Long {
        if (!store.containsKey(key)) return 0
        val removed = expirationTimes.remove(key)
        return if (removed != null) 1 else 0
    }

    fun incr(key: String): Result<Long> = incrBy(key, 1)

    fun decr(key: String): Result<Long> = incrBy(key, -1)

    fun incrBy(key: String, delta: Long): Result<Long> {
        val current = get(key)
        val currentValue = if (current == null) {
            0L
        } else {
            current.toString(Charsets.UTF_8).toLongOrNull()
                ?: return Result.failure(NotAnIntegerException())
        }
        val newValue = currentValue + delta
        store[key] = newValue.toString().toByteArray()
        return Result.success(newValue)
    }

    fun setNx(key: String, value: ByteArray): Boolean {
        if (isExpired(key)) {
            removeKey(key)
        }
        if (store.containsKey(key)) return false
        store[key] = value
        return true
    }

    fun mGet(keys: List<String>): List<ByteArray?> = keys.map { get(it) }

    fun mSet(entries: Map<String, ByteArray>) {
        entries.forEach { (key, value) ->
            store[key] = value
            expirationTimes.remove(key)
        }
    }

    fun pttl(key: String): Long {
        val expireAt = expirationTimes[key]
        if (!store.containsKey(key)) return KEY_NOT_EXISTS
        if (expireAt == null) return NO_TTL

        val remainingMillis = expireAt - clock.currentTimeMillis()
        if (remainingMillis <= 0) {
            removeKey(key)
            return KEY_NOT_EXISTS
        }
        return remainingMillis
    }

    fun pexpire(key: String, millis: Long): Long {
        if (!store.containsKey(key)) return 0
        if (millis <= 0) return delete(key)

        setExpiration(key, millis)
        return 1
    }

    fun keys(pattern: String): List<String> {
        cleanupExpiredKeysForIteration()
        val regex = patternToRegex(pattern)
        return store.keys.filter { regex.matches(it) }
    }

    fun scan(cursor: Long, pattern: String?, count: Int): Pair<Long, List<String>> {
        cleanupExpiredKeysForIteration()
        val allKeys = store.keys.toList().sorted()
        if (allKeys.isEmpty()) return Pair(0L, emptyList())

        val startIndex = cursor.toInt().coerceIn(0, allKeys.size)
        val regex = pattern?.let { patternToRegex(it) }

        val result = mutableListOf<String>()
        var index = startIndex
        var scanned = 0

        while (scanned < count && index < allKeys.size) {
            val key = allKeys[index]
            if (regex == null || regex.matches(key)) {
                result.add(key)
            }
            index++
            scanned++
        }

        val nextCursor = if (index >= allKeys.size) 0L else index.toLong()
        return Pair(nextCursor, result)
    }

    private fun isExpired(key: String): Boolean {
        val expireAt = expirationTimes[key] ?: return false
        return expireAt <= clock.currentTimeMillis()
    }

    private fun removeKey(key: String) {
        store.remove(key)
        expirationTimes.remove(key)
    }

    private fun cleanupExpiredKeysForIteration() {
        val expiredKeys = expirationTimes.entries
            .filter { it.value <= clock.currentTimeMillis() }
            .map { it.key }
        expiredKeys.forEach { removeKey(it) }
    }

    private fun patternToRegex(pattern: String): Regex {
        val regexPattern = buildString {
            append("^")
            for (char in pattern) {
                when (char) {
                    '*' -> append(".*")
                    '?' -> append(".")
                    '[', ']', '(', ')', '{', '}', '.', '+', '^', '$', '|', '\\' -> {
                        append("\\")
                        append(char)
                    }
                    else -> append(char)
                }
            }
            append("$")
        }
        return regexPattern.toRegex()
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
