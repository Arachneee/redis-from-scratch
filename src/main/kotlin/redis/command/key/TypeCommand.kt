package redis.command.key

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations

class TypeCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = "TYPE"
    override val arity: Int = 2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 2) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()

        val type = keyOps.type(key)
        return RESPValue.SimpleString(type)
    }
}
