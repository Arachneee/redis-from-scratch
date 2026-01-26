package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.StringOperations

class SetExCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = NAME
    override val arity: Int = 4
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_DENYOOM)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 4) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val secondsStr = args.getStringAt(2) ?: return wrongArgsError()
        val value = args.getBytesAt(3) ?: return wrongArgsError()

        val seconds = secondsStr.toLongOrNull()
            ?: return RESPValue.Error("ERR value is not an integer or out of range")

        if (seconds <= 0) {
            return RESPValue.Error("ERR invalid expire time in 'setex' command")
        }

        stringOps.setWithTtlSeconds(key, value, seconds)
        return RESPValue.SimpleString("OK")
    }

    companion object {
        const val NAME = "SETEX"
    }
}
