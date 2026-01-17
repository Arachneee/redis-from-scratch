package redis.command.key

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations

class RenameCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = "RENAME"
    override val arity: Int = 3
    override val flags: List<String> = listOf("write")
    override val firstKey: Int = 1
    override val lastKey: Int = 2
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 3) return wrongArgsError()

        val key = args.getStringAt(1) ?: return wrongArgsError()
        val newKey = args.getStringAt(2) ?: return wrongArgsError()

        val success = keyOps.rename(key, newKey)
        return if (success) {
            RESPValue.SimpleString("OK")
        } else {
            RESPValue.Error("ERR no such key")
        }
    }
}
