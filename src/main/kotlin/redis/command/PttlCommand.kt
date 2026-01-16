package redis.command

import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.RedisRepository

class PttlCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "PTTL"
    override val arity: Int = 2
    override val flags: List<String> = listOf("readonly", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        return RESPValue.Integer(repository.pttl(key))
    }
}
