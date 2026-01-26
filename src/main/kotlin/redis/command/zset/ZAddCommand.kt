package redis.command.zset

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.ZSetOperations

class ZAddCommand(
    private val zsetOps: ZSetOperations,
) : RedisCommand {
    override val name: String = "ZADD"
    override val arity: Int = -4
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_DENYOOM, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 4 || (args.size - 2) % 2 != 0) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val members = mutableListOf<Pair<Double, ByteArray>>()

        var i = 2
        while (i < args.size) {
            val scoreStr = args.getStringAt(i) ?: return wrongArgsError()
            val score = scoreStr.toDoubleOrNull()
                ?: return RESPValue.Error("ERR value is not a valid float")
            val member = args.getBytesAt(i + 1) ?: return wrongArgsError()
            members.add(score to member)
            i += 2
        }

        val addedCount = zsetOps.zadd(key, members)
        return RESPValue.Integer(addedCount)
    }
}
