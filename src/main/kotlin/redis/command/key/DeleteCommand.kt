package redis.command.key

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringsFrom
import redis.storage.KeyOperations

class DeleteCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = "DEL"
    override val arity: Int = -2
    override val flags: List<String> = listOf(RedisCommand.FLAG_WRITE)
    override val firstKey: Int = 1
    override val lastKey: Int = -1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val keys = args.getStringsFrom(1)
        if (keys.isEmpty()) {
            return wrongArgsError()
        }
        val count = keyOps.delete(keys)
        return RESPValue.Integer(count)
    }
}
