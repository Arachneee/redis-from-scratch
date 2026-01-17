package redis.command.list

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.ListOperations

class LPushCommand(
    private val listOps: ListOperations,
) : RedisCommand {
    override val name: String = "LPUSH"
    override val arity: Int = -3
    override val flags: List<String> = listOf("write", "denyoom", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val values = (2 until args.size).mapNotNull { args.getBytesAt(it) }

        if (values.isEmpty()) return wrongArgsError()

        val length = listOps.lpush(key, values)
        return RESPValue.Integer(length)
    }
}
