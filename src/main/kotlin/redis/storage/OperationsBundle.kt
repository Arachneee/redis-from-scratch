package redis.storage

class OperationsBundle(
    val string: StringOperations,
    val key: KeyOperations,
    val server: ServerOperations,
) {
    companion object {
        fun create(clock: Clock = SystemClock): OperationsBundle {
            val store = RedisStore(clock)
            return OperationsBundle(
                string = StringOperations(store),
                key = KeyOperations(store),
                server = ServerOperations(store),
            )
        }
    }
}
