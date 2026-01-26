package redis.command.list

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.ListOperations

class LPopCommand(
    private val listOps: ListOperations,
) : RedisCommand {
    override val name: String = "LPOP"
    override val arity: Int = -2
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 2) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val count = args.getStringAt(2)?.toIntOrNull() ?: 1

        if (count <= 0) return wrongArgsError()

        val result = listOps.lpop(key, count)

        return if (result.isEmpty()) {
            RESPValue.BulkString(null)
        } else if (args.size == 2) {
            RESPValue.BulkString(result.first())
        } else {
            RESPValue.Array(result.map { RESPValue.BulkString(it) })
        }
    }
}
