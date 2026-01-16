package redis.command

import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.RedisRepository

class KeysCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "KEYS"
    override val arity: Int = 2
    override val flags: List<String> = listOf("readonly", "sort_for_script")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        val pattern = args.getStringAt(1) ?: return wrongArgsError()
        val keys = repository.keys(pattern)
        return RESPValue.Array(keys.map { RESPValue.BulkString(it.toByteArray()) })
    }
}
