package redis.command.string

import redis.command.RedisCommand
import redis.error.RedisErrors
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.NotAnIntegerException
import redis.storage.StringOperations

class IncrByCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = "INCRBY"
    override val arity: Int = 3
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_DENYOOM, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val delta = args.getStringAt(2)?.toLongOrNull() ?: return RedisErrors.invalidInteger()
        return stringOps.incrBy(key, delta).fold(
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
