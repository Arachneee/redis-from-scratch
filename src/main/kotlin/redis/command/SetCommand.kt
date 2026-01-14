package redis.command

import redis.RedisRepository
import redis.protocol.RESPValue

class SetCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "SET"
    override val arity: Int = -3
    override val flags: List<String> = listOf("write", "denyoom")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = (args.getOrNull(1) as? RESPValue.BulkString)?.asString
        val value = (args.getOrNull(2) as? RESPValue.BulkString)?.data
        if (key == null || value == null) {
            return RESPValue.Error("ERR wrong number of arguments for 'set' command")
        }
        repository.set(key, value)
        return RESPValue.SimpleString("OK")
    }
}
