package redis.command.hash

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.HashOperations

class HSetCommand(
    private val hashOps: HashOperations,
) : RedisCommand {
    override val name: String = "HSET"
    override val arity: Int = -4
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_DENYOOM, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 4 || (args.size - 2) % 2 != 0) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val fieldValues = mutableListOf<Pair<String, ByteArray>>()

        var i = 2
        while (i < args.size) {
            val field = args.getStringAt(i) ?: return wrongArgsError()
            val value = args.getBytesAt(i + 1) ?: return wrongArgsError()
            fieldValues.add(field to value)
            i += 2
        }

        val newFieldCount = hashOps.hset(key, fieldValues)
        return RESPValue.Integer(newFieldCount)
    }
}
