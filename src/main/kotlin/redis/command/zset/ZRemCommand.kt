package redis.command.zset

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.ZSetOperations

class ZRemCommand(
    private val zsetOps: ZSetOperations,
) : RedisCommand {
    override val name: String = "ZREM"
    override val arity: Int = -3
    override val flags: List<String> = listOf("write", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val members = (2 until args.size).mapNotNull { args.getBytesAt(it) }

        if (members.isEmpty()) return wrongArgsError()

        val removedCount = zsetOps.zrem(key, members)
        return RESPValue.Integer(removedCount)
    }
}
