package redis.command.string

import redis.command.RedisCommand
import redis.error.RedisErrors
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.StringOperations

class PSetExCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = NAME
    override val arity: Int = 4
    override val flags: List<String> = listOf("write", "denyoom")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 4) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val millisStr = args.getStringAt(2) ?: return wrongArgsError()
        val value = args.getBytesAt(3) ?: return wrongArgsError()

        val millis = millisStr.toLongOrNull()
            ?: return RedisErrors.invalidInteger()

        if (millis <= 0) {
            return RESPValue.Error("ERR invalid expire time in 'psetex' command")
        }

        stringOps.setWithTtlMillis(key, value, millis)
        return RESPValue.SimpleString("OK")
    }

    companion object {
        const val NAME = "PSETEX"
    }
}
