package redis.command

import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.RedisRepository

class PexpireCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "PEXPIRE"
    override val arity: Int = 3
    override val flags: List<String> = listOf("write", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val millis = args.getStringAt(2)?.toLongOrNull() ?: return wrongArgsError()
        return RESPValue.Integer(repository.pexpire(key, millis))
    }
}
