package redis.command

import redis.storage.RedisRepository
import redis.protocol.RESPValue
import redis.protocol.getStringAt

class ExpireCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "EXPIRE"
    override val arity: Int = 3
    override val flags: List<String> = listOf("write", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val seconds = args.getStringAt(2)?.toLongOrNull() ?: return wrongArgsError()

        val count = repository.expire(key, seconds)
        return RESPValue.Integer(count)
    }
}
