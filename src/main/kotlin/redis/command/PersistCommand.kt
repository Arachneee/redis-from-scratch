package redis.command

import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.RedisRepository

class PersistCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "PERSIST"
    override val arity: Int = 2
    override val flags: List<String> = listOf("write", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        return RESPValue.Integer(repository.persist(key))
    }
}
