package redis.command.hash

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.HashOperations

class HGetAllCommand(
    private val hashOps: HashOperations,
) : RedisCommand {
    override val name: String = "HGETALL"
    override val arity: Int = 2
    override val flags: List<String> = listOf("readonly")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 2) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()

        val entries = hashOps.hgetall(key)
        val result = mutableListOf<RESPValue>()
        entries.forEach { (field, value) ->
            result.add(RESPValue.BulkString(field.toByteArray()))
            result.add(RESPValue.BulkString(value))
        }
        return RESPValue.Array(result)
    }
}
