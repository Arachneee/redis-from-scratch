package redis.storage

import redis.error.WrongTypeException

class ListOperations(
    private val store: RedisStore,
) {
    fun lpush(
        key: String,
        values: List<ByteArray>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
        }
        val list = getOrCreateList(key)
        values.forEach { list.add(0, it) }
        return list.size.toLong()
    }

    fun rpush(
        key: String,
        values: List<ByteArray>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
        }
        val list = getOrCreateList(key)
        list.addAll(values)
        return list.size.toLong()
    }

    fun lpop(
        key: String,
        count: Int = 1,
    ): List<ByteArray> {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return emptyList()
        }
        val list = getExistingList(key) ?: return emptyList()
        val result = (0 until count).mapNotNull { list.removeFirstOrNull() }

        if (list.isEmpty()) store.removeKey(key)
        return result
    }

    fun rpop(
        key: String,
        count: Int = 1,
    ): List<ByteArray> {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return emptyList()
        }
        val list = getExistingList(key) ?: return emptyList()
        val result = (0 until count).mapNotNull { list.removeLastOrNull() }
        if (list.isEmpty()) store.removeKey(key)
        return result
    }

    fun lrange(
        key: String,
        start: Long,
        stop: Long,
    ): List<ByteArray> {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return emptyList()
        }
        val list = getExistingList(key) ?: return emptyList()
        val size = list.size

        val normalizedStart = normalizeIndex(start, size)
        val normalizedStop = normalizeIndex(stop, size)

        if (normalizedStart > normalizedStop || normalizedStart >= size) {
            return emptyList()
        }

        val endIndex = minOf(normalizedStop + 1, size)
        return list.subList(normalizedStart, endIndex).toList()
    }

    fun llen(key: String): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return 0L
        }
        val list = getExistingList(key) ?: return 0L
        return list.size.toLong()
    }

    private fun getOrCreateList(key: String): MutableList<ByteArray> {
        val existing = store.data[key]
        if (existing == null) {
            val newList = mutableListOf<ByteArray>()
            store.data[key] = newList
            return newList
        }
        return castToList(key, existing)
    }

    private fun getExistingList(key: String): MutableList<ByteArray>? {
        val existing = store.data[key] ?: return null
        return castToList(key, existing)
    }

    private fun castToList(
        key: String,
        value: Any,
    ): MutableList<ByteArray> {
        if (value is MutableList<*>) {
            @Suppress("UNCHECKED_CAST")
            return value as MutableList<ByteArray>
        }
        throw WrongTypeException(
            key = key,
            expectedType = "list",
            actualType = value::class.simpleName ?: "unknown",
        )
    }

    private fun normalizeIndex(
        index: Long,
        size: Int,
    ): Int =
        if (index < 0) {
            maxOf(0, size + index.toInt())
        } else {
            index.toInt()
        }
}
