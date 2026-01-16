package redis.command

import redis.protocol.RESPValue
import redis.storage.RedisRepository

class FlushDbCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "FLUSHDB"
    override val arity: Int = 1
    override val flags: List<String> = listOf("write")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        repository.flushAll()
        return RESPValue.SimpleString("OK")
    }
}
