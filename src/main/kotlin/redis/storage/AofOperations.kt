package redis.storage

sealed class SnapshotValue {
    data class String(
        val value: ByteArray,
    ) : SnapshotValue()

    data class List(
        val elements: kotlin.collections.List<ByteArray>,
    ) : SnapshotValue()

    data class Set(
        val members: kotlin.collections.List<ByteArray>,
    ) : SnapshotValue()

    data class Hash(
        val fields: Map<kotlin.String, ByteArray>,
    ) : SnapshotValue()

    data class ZSet(
        val members: kotlin.collections.List<Pair<ByteArray, Double>>,
    ) : SnapshotValue()
}

class AofOperations(
    private val store: RedisStore,
) {
    fun captureState(): Map<String, SnapshotValue> {
        store.cleanupExpiredKeysForIteration()
        return store.data.mapValues { (_, value) ->
            when (value) {
                is ByteArray -> {
                    SnapshotValue.String(value.copyOf())
                }

                is MutableList<*> -> {
                    @Suppress("UNCHECKED_CAST")
                    SnapshotValue.List((value as MutableList<ByteArray>).map { it.copyOf() })
                }

                is MutableSet<*> -> {
                    @Suppress("UNCHECKED_CAST")
                    SnapshotValue.Set((value as MutableSet<ByteArrayWrapper>).map { it.data.copyOf() })
                }

                is MutableMap<*, *> -> {
                    @Suppress("UNCHECKED_CAST")
                    SnapshotValue.Hash((value as MutableMap<String, ByteArray>).mapValues { it.value.copyOf() })
                }

                is ZSet -> {
                    SnapshotValue.ZSet(value.scoreMap.map { it.key.data.copyOf() to it.value })
                }

                else -> {
                    throw IllegalStateException("Unknown type: ${value::class.simpleName}")
                }
            }
        }
    }

    fun getExpiration(key: String): Long? = store.expirationTimes[key]
}
