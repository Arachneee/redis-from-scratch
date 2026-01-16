package redis.command

import redis.protocol.RESPValue
import redis.storage.RedisRepository

class DbSizeCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "DBSIZE"
    override val arity: Int = 1
    override val flags: List<String> = listOf("readonly", "fast")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        return RESPValue.Integer(repository.size())
    }
}
