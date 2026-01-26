package redis.command.zset

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.ZSetOperations

class ZRangeCommand(
    private val zsetOps: ZSetOperations,
) : RedisCommand {
    override val name: String = "ZRANGE"
    override val arity: Int = 4
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 4) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val startStr = args.getStringAt(2) ?: return wrongArgsError()
        val stopStr = args.getStringAt(3) ?: return wrongArgsError()

        val start = startStr.toLongOrNull()
            ?: return RESPValue.Error("ERR value is not an integer or out of range")
        val stop = stopStr.toLongOrNull()
            ?: return RESPValue.Error("ERR value is not an integer or out of range")

        val members = zsetOps.zrange(key, start, stop)
        val result = members.map { RESPValue.BulkString(it) }
        return RESPValue.Array(result)
    }
}
