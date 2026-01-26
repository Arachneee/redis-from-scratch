package redis.command.hash

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.HashOperations

class HExistsCommand(
    private val hashOps: HashOperations,
) : RedisCommand {
    override val name: String = "HEXISTS"
    override val arity: Int = 3
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val field = args.getStringAt(2) ?: return wrongArgsError()

        val exists = hashOps.hexists(key, field)
        return RESPValue.Integer(if (exists) 1L else 0L)
    }
}
