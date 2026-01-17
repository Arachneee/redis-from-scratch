package redis.storage

class ServerOperations(
    private val store: RedisStore,
) {
    fun size(): Long = store.size()

    fun flushAll() = store.flushAll()
}
