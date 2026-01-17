package redis.command.list

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.ListOperations

class LLenCommand(
    private val listOps: ListOperations,
) : RedisCommand {
    override val name: String = "LLEN"
    override val arity: Int = 2
    override val flags: List<String> = listOf("readonly", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 2) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val length = listOps.llen(key)
        return RESPValue.Integer(length)
    }
}
