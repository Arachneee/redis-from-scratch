package redis.command.set

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.SetOperations

class SMembersCommand(
    private val setOps: SetOperations,
) : RedisCommand {
    override val name: String = "SMEMBERS"
    override val arity: Int = 2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_SORT_FOR_SCRIPT)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 2) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()

        val members = setOps.smembers(key)
        val result = members.map { RESPValue.BulkString(it) }
        return RESPValue.Array(result)
    }
}
