package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.StringOperations

class SetNxCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = "SETNX"
    override val arity: Int = 3
    override val flags: List<String> = listOf("write", "denyoom", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val value = args.getBytesAt(2) ?: return wrongArgsError()
        val result = stringOps.setNx(key, value)
        return RESPValue.Integer(if (result) 1 else 0)
    }
}
