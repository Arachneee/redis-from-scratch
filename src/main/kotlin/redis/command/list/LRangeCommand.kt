package redis.command.list

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.ListOperations

class LRangeCommand(
    private val listOps: ListOperations,
) : RedisCommand {
    override val name: String = "LRANGE"
    override val arity: Int = 4
    override val flags: List<String> = listOf("readonly")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 4) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val start = args.getStringAt(2)?.toLongOrNull() ?: return wrongArgsError()
        val stop = args.getStringAt(3)?.toLongOrNull() ?: return wrongArgsError()

        val result = listOps.lrange(key, start, stop)
        return RESPValue.Array(result.map { RESPValue.BulkString(it) })
    }
}
