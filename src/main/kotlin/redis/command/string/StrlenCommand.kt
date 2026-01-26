package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.StringOperations

class StrlenCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = "STRLEN"
    override val arity: Int = 2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 2) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()

        val length = stringOps.strlen(key)
        return RESPValue.Integer(length)
    }
}
