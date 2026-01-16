package redis.command

import redis.storage.RedisRepository
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

        val option = args.getStringAt(3)?.uppercase()
        if (option == null) {
            repository.set(key, value)
            return OK
        }

        val ttl = args.getStringAt(4)?.toLongOrNull() ?: return wrongArgsError()
        if (ttl <= 0) return wrongArgsError()

        when (option) {
            OPTION_EX -> repository.setWithTtlSeconds(key, value, ttl)
            OPTION_PX -> repository.setWithTtlMillis(key, value, ttl)
            else -> return wrongArgsError()
        }
        return OK
    }

    companion object {
        private const val OPTION_EX = "EX"
        private const val OPTION_PX = "PX"
        private val OK = RESPValue.SimpleString("OK")
    }
}
