package redis.storage

import redis.error.WrongTypeException
import java.util.TreeSet

class ZSetOperations(
    private val store: RedisStore,
) {
    fun zadd(
        key: String,
        members: List<Pair<Double, ByteArray>>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
        }
        val zset = getOrCreateZSet(key)
        var addedCount = 0L
        members.forEach { (score, member) ->
            val wrapper = ByteArrayWrapper(member)
            val existingEntry = zset.scoreMap[wrapper]
            if (existingEntry == null) {
                addedCount++
            } else {
                zset.sortedSet.remove(ZSetEntry(existingEntry, wrapper))
            }
            zset.scoreMap[wrapper] = score
            zset.sortedSet.add(ZSetEntry(score, wrapper))
        }
        return addedCount
    }

    fun zrem(
        key: String,
        members: List<ByteArray>,
    ): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return 0L
        }
        val zset = getExistingZSet(key) ?: return 0L
        var removedCount = 0L
        members.forEach { member ->
            val wrapper = ByteArrayWrapper(member)
            val score = zset.scoreMap.remove(wrapper)
            if (score != null) {
                zset.sortedSet.remove(ZSetEntry(score, wrapper))
                removedCount++
            }
        }
        if (zset.scoreMap.isEmpty()) {
            store.removeKey(key)
        }
        return removedCount
    }

    fun zscore(
        key: String,
        member: ByteArray,
    ): Double? {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return null
        }
        val zset = getExistingZSet(key) ?: return null
        return zset.scoreMap[ByteArrayWrapper(member)]
    }

    fun zrank(
        key: String,
        member: ByteArray,
    ): Long? {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return null
        }
        val zset = getExistingZSet(key) ?: return null
        val wrapper = ByteArrayWrapper(member)
        val score = zset.scoreMap[wrapper] ?: return null

        var rank = 0L
        for (entry in zset.sortedSet) {
            if (entry.member == wrapper) {
                return rank
            }
            rank++
        }
        return null
    }

    fun zrange(
        key: String,
        start: Long,
        stop: Long,
    ): List<ByteArray> {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return emptyList()
        }
        val zset = getExistingZSet(key) ?: return emptyList()
        val size = zset.sortedSet.size

        val normalizedStart = normalizeIndex(start, size)
        val normalizedStop = normalizeIndex(stop, size)

        if (normalizedStart > normalizedStop || normalizedStart >= size) {
            return emptyList()
        }

        val endIndex = minOf(normalizedStop + 1, size)
        return zset.sortedSet
            .drop(normalizedStart)
            .take(endIndex - normalizedStart)
            .map { it.member.data }
    }

    fun zcard(key: String): Long {
        if (store.isExpired(key)) {
            store.removeKey(key)
            return 0L
        }
        val zset = getExistingZSet(key) ?: return 0L
        return zset.scoreMap.size.toLong()
    }

    private fun getOrCreateZSet(key: String): ZSet {
        val existing = store.data[key]
        if (existing == null) {
            val newZSet = ZSet()
            store.data[key] = newZSet
            return newZSet
        }
        return castToZSet(key, existing)
    }

    private fun getExistingZSet(key: String): ZSet? {
        val existing = store.data[key] ?: return null
        return castToZSet(key, existing)
    }

    private fun castToZSet(
        key: String,
        value: Any,
    ): ZSet {
        if (value is ZSet) {
            return value
        }
        throw WrongTypeException(
            key = key,
            expectedType = "zset",
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

class ZSet {
    val scoreMap = mutableMapOf<ByteArrayWrapper, Double>()
    val sortedSet: TreeSet<ZSetEntry> = TreeSet()
}

data class ZSetEntry(
    val score: Double,
    val member: ByteArrayWrapper,
) : Comparable<ZSetEntry> {
    override fun compareTo(other: ZSetEntry): Int {
        val scoreCompare = score.compareTo(other.score)
        if (scoreCompare != 0) return scoreCompare
        return compareByteArrays(member.data, other.member.data)
    }

    private fun compareByteArrays(
        a: ByteArray,
        b: ByteArray,
    ): Int {
        val minLength = minOf(a.size, b.size)
        for (i in 0 until minLength) {
            val cmp = (a[i].toInt() and 0xFF).compareTo(b[i].toInt() and 0xFF)
            if (cmp != 0) return cmp
        }
        return a.size.compareTo(b.size)
    }
}
