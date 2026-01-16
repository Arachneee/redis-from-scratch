package redis.command

import redis.storage.RedisRepository
import redis.protocol.RESPValue
import redis.protocol.getStringsFrom

class DeleteCommand(
    private val repository: RedisRepository,
) : RedisCommand {
    override val name: String = "DEL"
    override val arity: Int = -2
    override val flags: List<String> = listOf("write")
    override val firstKey: Int = 1
    override val lastKey: Int = -1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val keys = args.getStringsFrom(1)
        if (keys.isEmpty()) {
            return wrongArgsError()
        }
        val count = repository.delete(keys)
        return RESPValue.Integer(count)
    }
}
