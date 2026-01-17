package redis.storage

import redis.error.WrongTypeException

class StringOperations(
    private val store: RedisStore,
) {
    fun get(key: String): ByteArray? {
        if (store.expirationTimes.getOrDefault(key, Long.MAX_VALUE) > store.clock.currentTimeMillis()) {
            val value = store.data[key] ?: return null
            if (value is ByteArray) {
                return value
            }
            throw WrongTypeException(
                key = key,
                expectedType = "string",
                actualType = value::class.simpleName ?: "unknown",
            )
        } else {
            store.removeKey(key)
            return null
        }
    }

    fun set(key: String, value: ByteArray) {
        store.data[key] = value
        store.expirationTimes.remove(key)
    }

    fun setWithTtlSeconds(key: String, value: ByteArray, ttlSeconds: Long) {
        store.data[key] = value
        store.setExpiration(key, ttlSeconds * 1000)
    }

    fun setWithTtlMillis(key: String, value: ByteArray, ttlMillis: Long) {
        store.data[key] = value
        store.setExpiration(key, ttlMillis)
    }

    fun incr(key: String): Result<Long> = incrBy(key, 1)

    fun decr(key: String): Result<Long> = incrBy(key, -1)

    fun incrBy(key: String, delta: Long): Result<Long> {
        val current = get(key)
        val currentValue =
            if (current == null) {
                0L
            } else {
                current.toString(Charsets.UTF_8).toLongOrNull()
                    ?: return Result.failure(NotAnIntegerException())
            }
        val newValue = currentValue + delta
        store.data[key] = newValue.toString().toByteArray()
        return Result.success(newValue)
    }

    fun setNx(key: String, value: ByteArray): Boolean {
        if (store.isExpired(key)) {
            store.removeKey(key)
        }
        if (store.containsKey(key)) return false
        store.data[key] = value
        return true
    }

    fun mGet(keys: List<String>): List<ByteArray?> = keys.map { get(it) }

    fun mSet(entries: Map<String, ByteArray>) {
        entries.forEach { (key, value) ->
            store.data[key] = value
            store.expirationTimes.remove(key)
        }
    }
}
