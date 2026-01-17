package redis.storage

import redis.error.WrongTypeException

class SetOperations(
    private val store: RedisStore,
) {
    fun sadd(
        key: String,
        members: List<ByteArray>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
        }
        val set = getOrCreateSet(key)
        var addedCount = 0L
        members.forEach { member ->
            if (set.add(ByteArrayWrapper(member))) {
                addedCount++
            }
        }
        return addedCount
    }

    fun srem(
        key: String,
        members: List<ByteArray>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return 0L
        }
        val set = getExistingSet(key) ?: return 0L
        var removedCount = 0L
        members.forEach { member ->
            if (set.remove(ByteArrayWrapper(member))) {
                removedCount++
            }
        }
        if (set.isEmpty()) {
            store.removeKey(key)
        }
        return removedCount
    }

    fun smembers(key: String): Set<ByteArray> {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return emptySet()
        }
        val set = getExistingSet(key) ?: return emptySet()
        return set.map { it.data }.toSet()
    }

    fun sismember(
        key: String,
        member: ByteArray,
    ): Boolean {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return false
        }
        val set = getExistingSet(key) ?: return false
        return set.contains(ByteArrayWrapper(member))
    }

    fun scard(key: String): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return 0L
        }
        val set = getExistingSet(key) ?: return 0L
        return set.size.toLong()
    }

    private fun getOrCreateSet(key: String): MutableSet<ByteArrayWrapper> {
        val existing = store.data[key]
        if (existing == null) {
            val newSet = mutableSetOf<ByteArrayWrapper>()
            store.data[key] = newSet
            return newSet
        }
        return castToSet(key, existing)
    }

    private fun getExistingSet(key: String): MutableSet<ByteArrayWrapper>? {
        val existing = store.data[key] ?: return null
        return castToSet(key, existing)
    }

    private fun castToSet(
        key: String,
        value: Any,
    ): MutableSet<ByteArrayWrapper> {
        if (value is MutableSet<*>) {
            @Suppress("UNCHECKED_CAST")
            return value as MutableSet<ByteArrayWrapper>
        }
        throw WrongTypeException(
            key = key,
            expectedType = "set",
            actualType = value::class.simpleName ?: "unknown",
        )
    }
}

class ByteArrayWrapper(
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ByteArrayWrapper) return false
        return data.contentEquals(other.data)
    }

    override fun hashCode(): Int = data.contentHashCode()
}
