package redis.command.key

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations

class PexpireAtCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = NAME
    override val arity: Int = -3
    override val flags: List<String> = listOf("write", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val timestampMillis = args.getStringAt(2)?.toLongOrNull() ?: return wrongArgsError()

        val count = keyOps.expireAt(key, timestampMillis)
        return RESPValue.Integer(count)
    }

    companion object {
        const val NAME = "PEXPIREAT"
    }
}
