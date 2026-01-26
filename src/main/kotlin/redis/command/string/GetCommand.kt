package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.StringOperations

class GetCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = "GET"
    override val arity: Int = 2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val value = stringOps.get(key)
        return RESPValue.BulkString(value)
    }
}
