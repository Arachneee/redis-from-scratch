package redis.command.string

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringsFrom
import redis.storage.StringOperations

class MGetCommand(
    private val stringOps: StringOperations,
) : RedisCommand {
    override val name: String = "MGET"
    override val arity: Int = -2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = -1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val keys = args.getStringsFrom(1)
        if (keys.isEmpty()) return wrongArgsError()
        val values = stringOps.mGet(keys)
        return RESPValue.Array(values.map { RESPValue.BulkString(it) })
    }
}
