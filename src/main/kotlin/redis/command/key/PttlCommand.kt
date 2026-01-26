package redis.command.key

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations

class PttlCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = "PTTL"
    override val arity: Int = 2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        return RESPValue.Integer(keyOps.pttl(key))
    }
}
