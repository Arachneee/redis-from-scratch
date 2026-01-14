package redis.command

import redis.RedisRepository
import redis.protocol.RESPValue

class GetCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "GET"
    override val arity: Int = 2
    override val flags: List<String> = listOf("readonly", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = (args.getOrNull(1) as? RESPValue.BulkString)?.asString
            ?: return RESPValue.Error("ERR wrong number of arguments for 'get' command")
        val value = repository.get(key)
        return RESPValue.BulkString(value)
    }
}
