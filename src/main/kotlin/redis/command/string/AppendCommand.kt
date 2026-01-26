package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.StringOperations

class AppendCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = "APPEND"
    override val arity: Int = 3
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_DENYOOM, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val value = args.getBytesAt(2) ?: return wrongArgsError()

        val newLength = stringOps.append(key, value)
        return RESPValue.Integer(newLength)
    }
}
