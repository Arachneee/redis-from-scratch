package redis.command

import redis.RedisRepository
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt

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
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val value = args.getBytesAt(2) ?: return wrongArgsError()
        repository.set(key, value)
        return RESPValue.SimpleString("OK")
    }
}
