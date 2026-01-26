package redis.command.hash

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.HashOperations

class HDelCommand(
    private val hashOps: HashOperations,
) : RedisCommand {
    override val name: String = "HDEL"
    override val arity: Int = -3
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val fields = (2 until args.size).mapNotNull { args.getStringAt(it) }

        if (fields.isEmpty()) return wrongArgsError()

        val deletedCount = hashOps.hdel(key, fields)
        return RESPValue.Integer(deletedCount)
    }
}
