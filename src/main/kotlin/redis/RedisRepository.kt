package redis

import java.util.concurrent.ConcurrentHashMap

class RedisRepository {
    private val store = ConcurrentHashMap<String, ByteArray>()

    fun get(key: String): ByteArray? = store[key]

    fun set(
        key: String,
        value: ByteArray,
    ) {
        store[key] = value
    }

    fun delete(key: String): Long {
        if (!store.containsKey(key)) return 0
        store.remove(key)
        return 1
    }

    fun delete(keys: Collection<String>): Long = keys.sumOf { delete(it) }
}
