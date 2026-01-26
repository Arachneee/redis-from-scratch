package redis.command.key

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations

class KeysCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = "KEYS"
    override val arity: Int = 2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_SORT_FOR_SCRIPT)
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        val pattern = args.getStringAt(1) ?: return wrongArgsError()
        val keys = keyOps.keys(pattern)
        return RESPValue.Array(keys.map { RESPValue.BulkString(it.toByteArray()) })
    }
}
