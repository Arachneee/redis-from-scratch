package redis.command

import redis.error.RedisErrors
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.NotAnIntegerException
import redis.storage.RedisRepository

class IncrCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "INCR"
    override val arity: Int = 2
    override val flags: List<String> = listOf("write", "denyoom", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        return repository.incr(key).fold(
            onSuccess = { RESPValue.Integer(it) },
            onFailure = { error ->
                when (error) {
                    is NotAnIntegerException -> RedisErrors.invalidInteger()
                    else -> RESPValue.Error("ERR ${error.message}")
                }
            }
        )
    }
}
