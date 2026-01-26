package redis.command.hash

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.HashOperations

class HGetCommand(
    private val hashOps: HashOperations,
) : RedisCommand {
    override val name: String = "HGET"
    override val arity: Int = 3
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val field = args.getStringAt(2) ?: return wrongArgsError()

        val value = hashOps.hget(key, field)
        return if (value != null) {
            RESPValue.BulkString(value)
        } else {
            RESPValue.BulkString(null)
        }
    }
}
