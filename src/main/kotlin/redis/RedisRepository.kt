package redis

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

class RedisRepository {
    private val store = ConcurrentHashMap<String, ByteArray>()
    private val ttlMillis = ConcurrentHashMap<String, Long>()
    private val cleanupScope = CoroutineScope(Dispatchers.Default)

    init {
        cleanupScope.launch {
            while (true) {
                val now = System.currentTimeMillis()
                val randomKeys = ttlMillis.keys.shuffled().take(TTL_EXPIRE_SAMPLES)

                val count =
                    randomKeys
                        .asSequence()
                        .filter { ttlMillis.getOrDefault(it, Long.MAX_VALUE) < now }
                        .onEach {
                            store.remove(it)
                            ttlMillis.remove(it)
                        }.count()

                if (count == 0 || count <= TTL_EXPIRE_SAMPLES / 4) {
                    delay(100)
                }
            }
        }
    }

    fun get(key: String): ByteArray? {
        if (ttlMillis.getOrDefault(key, Long.MAX_VALUE) > System.currentTimeMillis()) {
            return store[key]
        } else {
            store.remove(key)
            ttlMillis.remove(key)
            return null
        }
    }

    fun set(
        key: String,
        value: ByteArray,
    ) {
        store[key] = value
        ttlMillis.remove(key)
    }

    fun delete(key: String): Long {
        if (!store.containsKey(key)) return 0
        store.remove(key)
        ttlMillis.remove(key)
        return 1
    }

    fun delete(keys: Collection<String>): Long = keys.sumOf { delete(it) }

    fun expire(
        key: String,
        seconds: Long,
    ): Long {
        if (!store.containsKey(key)) return 0
        if (seconds <= 0) return delete(key)

        ttlMillis[key] = seconds * 1000 + System.currentTimeMillis()
        return 1
    }

    companion object {
        private const val TTL_EXPIRE_SAMPLES = 20
    }
}
