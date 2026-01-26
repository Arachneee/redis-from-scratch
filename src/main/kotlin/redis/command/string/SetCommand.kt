package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.StringOperations

class SetCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = NAME
    override val arity: Int = -3
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_DENYOOM)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val value = args.getBytesAt(2) ?: return wrongArgsError()

        if (args.size == 3) {
            stringOps.set(key, value)
            return OK
        }

        var i = 3
        var expirationSet = false
        while (i < args.size) {
            val option = args.getStringAt(i)?.uppercase()
            when (option) {
                OPTION_EX -> {
                    val ttl = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    if (ttl <= 0) return wrongArgsError()
                    stringOps.setWithTtlSeconds(key, value, ttl)
                    expirationSet = true
                    i += 2
                }
                OPTION_PX -> {
                    val ttl = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    if (ttl <= 0) return wrongArgsError()
                    stringOps.setWithTtlMillis(key, value, ttl)
                    expirationSet = true
                    i += 2
                }
                OPTION_EXAT -> {
                    val timestamp = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    stringOps.setWithExpirationAtMillis(key, value, timestamp * 1000)
                    expirationSet = true
                    i += 2
                }
                OPTION_PXAT -> {
                    val timestamp = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    stringOps.setWithExpirationAtMillis(key, value, timestamp)
                    expirationSet = true
                    i += 2
                }
                "NX", "XX" -> {
                    // TODO: Implement NX/XX support for SET if needed
                    // For now, just skip to avoid erroring out
                    i += 1
                }
                else -> return wrongArgsError()
            }
        }

        if (!expirationSet) {
            stringOps.set(key, value)
        }
        return OK
    }

    companion object {
        const val NAME = "SET"
        const val OPTION_EX = "EX"
        const val OPTION_PX = "PX"
        const val OPTION_EXAT = "EXAT"
        const val OPTION_PXAT = "PXAT"
        private val OK = RESPValue.SimpleString("OK")
    }
}
