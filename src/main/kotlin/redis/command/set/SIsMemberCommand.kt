package redis.command.set

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.SetOperations

class SIsMemberCommand(
    private val setOps: SetOperations,
) : RedisCommand {
    override val name: String = "SISMEMBER"
    override val arity: Int = 3
    override val flags: List<String> = listOf("readonly", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val member = args.getBytesAt(2) ?: return wrongArgsError()

        val isMember = setOps.sismember(key, member)
        return RESPValue.Integer(if (isMember) 1L else 0L)
    }
}
