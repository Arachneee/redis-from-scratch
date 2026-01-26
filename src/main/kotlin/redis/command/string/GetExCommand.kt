package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations
import redis.storage.StringOperations

class GetExCommand(
    private val stringOps: StringOperations,
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = NAME
    override val arity: Int = -2
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val value = stringOps.get(key) ?: return RESPValue.BulkString(null)

        if (args.size == 2) {
            return RESPValue.BulkString(value)
        }

        var i = 2
        while (i < args.size) {
            val option = args.getStringAt(i)?.uppercase()
            when (option) {
                "EX" -> {
                    val seconds = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    keyOps.expire(key, seconds)
                    i += 2
                }

                "PX" -> {
                    val millis = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    keyOps.pexpire(key, millis)
                    i += 2
                }

                "EXAT" -> {
                    val timestampSeconds = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    val nowSeconds = System.currentTimeMillis() / 1000
                    keyOps.expire(key, timestampSeconds - nowSeconds)
                    i += 2
                }

                "PXAT" -> {
                    val timestampMillis = args.getStringAt(i + 1)?.toLongOrNull() ?: return wrongArgsError()
                    val nowMillis = System.currentTimeMillis()
                    keyOps.pexpire(key, timestampMillis - nowMillis)
                    i += 2
                }

                "PERSIST" -> {
                    keyOps.persist(key)
                    i += 1
                }

                else -> {
                    return wrongArgsError()
                }
            }
        }

        return RESPValue.BulkString(value)
    }

    companion object {
        const val NAME = "GETEX"
    }
}
