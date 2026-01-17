package redis.storage

class KeyOperations(
    private val store: RedisStore,
) {
    fun type(key: String): String {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return "none"
        }
        val value = store.data[key] ?: return "none"
        return when (value) {
            is ByteArray -> "string"
            is MutableList<*> -> "list"
            is MutableSet<*> -> "set"
            is MutableMap<*, *> -> "hash"
            is ZSet -> "zset"
            else -> "unknown"
        }
    }

    fun rename(
        key: String,
        newKey: String,
    ): Boolean {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return false
        }
        val value = store.data.remove(key) ?: return false
        val expiration = store.expirationTimes.remove(key)

        store.data[newKey] = value
        if (expiration != null) {
            store.expirationTimes[newKey] = expiration
        } else {
            store.expirationTimes.remove(newKey)
        }
        return true
    }

    fun delete(key: String): Long {
        val removed = store.data.remove(key)
        store.expirationTimes.remove(key)
        return if (removed != null) 1 else 0
    }

    fun delete(keys: Collection<String>): Long = keys.sumOf { delete(it) }

    fun expire(key: String, seconds: Long): Long {
        if (!store.containsKey(key)) return 0
        if (seconds <= 0) return delete(key)

        store.setExpiration(key, seconds * 1000)
        return 1
    }

    fun pexpire(key: String, millis: Long): Long {
        if (!store.containsKey(key)) return 0
        if (millis <= 0) return delete(key)

        store.setExpiration(key, millis)
        return 1
    }

    fun ttl(key: String): Long {
        val expireAt = store.expirationTimes[key]
        if (!store.containsKey(key)) return RedisStore.KEY_NOT_EXISTS
        if (expireAt == null) return RedisStore.NO_TTL

        val remainingMillis = expireAt - store.clock.currentTimeMillis()
        if (remainingMillis <= 0) {
            store.removeKey(key)
            return RedisStore.KEY_NOT_EXISTS
        }
        return (remainingMillis + 999) / 1000
    }

    fun pttl(key: String): Long {
        val expireAt = store.expirationTimes[key]
        if (!store.containsKey(key)) return RedisStore.KEY_NOT_EXISTS
        if (expireAt == null) return RedisStore.NO_TTL

        val remainingMillis = expireAt - store.clock.currentTimeMillis()
        if (remainingMillis <= 0) {
            store.removeKey(key)
            return RedisStore.KEY_NOT_EXISTS
        }
        return remainingMillis
    }

    fun exists(keys: Collection<String>): Long =
        keys
            .count { key ->
                val expireAt = store.expirationTimes[key]
                if (expireAt != null && expireAt <= store.clock.currentTimeMillis()) {
                    store.removeKey(key)
                    false
                } else {
                    store.containsKey(key)
                }
            }.toLong()

    fun persist(key: String): Long {
        if (!store.containsKey(key)) return 0
        val removed = store.expirationTimes.remove(key)
        return if (removed != null) 1 else 0
    }

    fun keys(pattern: String): List<String> {
        store.cleanupExpiredKeysForIteration()
        val regex = patternToRegex(pattern)
        return store.data.keys.filter { regex.matches(it) }
    }

    fun scan(cursor: Long, pattern: String?, count: Int): Pair<Long, List<String>> {
        store.cleanupExpiredKeysForIteration()
        val allKeys = store.data.keys.toList().sorted()
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

    fun cleanupExpiredKeys(): Boolean {
        val keys = store.expirationTimes.keys.toList()
        if (keys.isEmpty()) return false

        val now = store.clock.currentTimeMillis()
        val randomKeys = keys.shuffled().take(TTL_EXPIRE_SAMPLES)

        val count =
            randomKeys
                .asSequence()
                .filter { store.expirationTimes.getOrDefault(it, Long.MAX_VALUE) < now }
                .onEach { store.removeKey(it) }
                .count()

        return count > TTL_EXPIRE_SAMPLES / 4
    }

    private fun patternToRegex(pattern: String): Regex {
        val regexPattern =
            buildString {
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

    companion object {
        private const val TTL_EXPIRE_SAMPLES = 20
    }
}
