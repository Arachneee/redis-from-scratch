package redis.command

import redis.protocol.RESPValue
import redis.protocol.getStringsFrom
import redis.storage.RedisRepository

class MGetCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "MGET"
    override val arity: Int = -2
    override val flags: List<String> = listOf("readonly", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = -1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val keys = args.getStringsFrom(1)
        if (keys.isEmpty()) return wrongArgsError()
        val values = repository.mGet(keys)
        return RESPValue.Array(values.map { RESPValue.BulkString(it) })
    }
}
