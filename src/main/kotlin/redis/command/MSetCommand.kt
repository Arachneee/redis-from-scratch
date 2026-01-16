package redis.command

import redis.protocol.RESPValue
import redis.protocol.getBytesAt
import redis.protocol.getStringAt
import redis.storage.RedisRepository

class MSetCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "MSET"
    override val arity: Int = -3
    override val flags: List<String> = listOf("write", "denyoom")
    override val firstKey: Int = 1
    override val lastKey: Int = -1
    override val step: Int = 2

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 3 || (args.size - 1) % 2 != 0) return wrongArgsError()

        val entries = mutableMapOf<String, ByteArray>()
        var i = 1
        while (i < args.size) {
            val key = args.getStringAt(i) ?: return wrongArgsError()
            val value = args.getBytesAt(i + 1) ?: return wrongArgsError()
            entries[key] = value
            i += 2
        }

        repository.mSet(entries)
        return RESPValue.SimpleString("OK")
    }
}
