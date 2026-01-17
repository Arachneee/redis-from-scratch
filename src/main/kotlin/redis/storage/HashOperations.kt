package redis.storage

import redis.error.WrongTypeException

class HashOperations(
    private val store: RedisStore,
) {
    fun hset(
        key: String,
        fieldValues: List<Pair<String, ByteArray>>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
        }
        val hash = getOrCreateHash(key)
        var newFieldCount = 0L
        fieldValues.forEach { (field, value) ->
            if (!hash.containsKey(field)) {
                newFieldCount++
            }
            hash[field] = value
        }
        return newFieldCount
    }

    fun hget(
        key: String,
        field: String,
    ): ByteArray? {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return null
        }
        val hash = getExistingHash(key) ?: return null
        return hash[field]
    }

    fun hdel(
        key: String,
        fields: List<String>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return 0L
        }
        val hash = getExistingHash(key) ?: return 0L
        var deletedCount = 0L
        fields.forEach { field ->
            if (hash.remove(field) != null) {
                deletedCount++
            }
        }
        if (hash.isEmpty()) {
            store.removeKey(key)
        }
        return deletedCount
    }

    fun hexists(
        key: String,
        field: String,
    ): Boolean {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return false
        }
        val hash = getExistingHash(key) ?: return false
        return hash.containsKey(field)
    }

    fun hlen(key: String): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return 0L
        }
        val hash = getExistingHash(key) ?: return 0L
        return hash.size.toLong()
    }

    fun hgetall(key: String): List<Pair<String, ByteArray>> {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return emptyList()
        }
        val hash = getExistingHash(key) ?: return emptyList()
        return hash.entries.map { it.key to it.value }
    }

    private fun getOrCreateHash(key: String): MutableMap<String, ByteArray> {
        val existing = store.data[key]
        if (existing == null) {
            val newHash = mutableMapOf<String, ByteArray>()
            store.data[key] = newHash
            return newHash
        }
        return castToHash(key, existing)
    }

    private fun getExistingHash(key: String): MutableMap<String, ByteArray>? {
        val existing = store.data[key] ?: return null
        return castToHash(key, existing)
    }

    private fun castToHash(
        key: String,
        value: Any,
    ): MutableMap<String, ByteArray> {
        if (value is MutableMap<*, *>) {
            @Suppress("UNCHECKED_CAST")
            return value as MutableMap<String, ByteArray>
        }
        throw WrongTypeException(
            key = key,
            expectedType = "hash",
            actualType = value::class.simpleName ?: "unknown",
        )
    }
}
