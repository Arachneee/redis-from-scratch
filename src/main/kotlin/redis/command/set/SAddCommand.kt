package redis.command.set

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.SetOperations

class SAddCommand(
    private val setOps: SetOperations,
) : RedisCommand {
    override val name: String = "SADD"
    override val arity: Int = -3
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_DENYOOM, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val members = (2 until args.size).mapNotNull { args.getBytesAt(it) }

        if (members.isEmpty()) return wrongArgsError()

        val addedCount = setOps.sadd(key, members)
        return RESPValue.Integer(addedCount)
    }
}
