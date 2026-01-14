package redis.command

import redis.RedisRepository
import redis.protocol.RESPValue

class DeleteCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "DELETE"
    override val arity: Int = -2
    override val flags: List<String> = listOf("write")
    override val firstKey: Int = 1
    override val lastKey: Int = -1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = (args.getOrNull(1) as? RESPValue.BulkString)?.asString
            ?: return RESPValue.Error("ERR wrong number of arguments for 'delete' command")
        val count = repository.delete(key)
        return RESPValue.SimpleString("$count")
    }
}
