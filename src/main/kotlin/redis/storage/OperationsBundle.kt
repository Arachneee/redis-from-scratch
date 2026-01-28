package redis.storage

class OperationsBundle(
    val string: StringOperations,
    val key: KeyOperations,
    val server: ServerOperations,
    val list: ListOperations,
    val hash: HashOperations,
    val set: SetOperations,
    val zset: ZSetOperations,
    val aof: AofOperations,
) {
    companion object {
        fun create(clock: Clock = SystemClock): OperationsBundle {
            val store = RedisStore(clock)
            return OperationsBundle(
                string = StringOperations(store),
                key = KeyOperations(store),
                server = ServerOperations(store),
                list = ListOperations(store),
                hash = HashOperations(store),
                set = SetOperations(store),
                zset = ZSetOperations(store),
                aof = AofOperations(store),
            )
        }
    }
}
