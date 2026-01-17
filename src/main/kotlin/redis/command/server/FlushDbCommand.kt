package redis.command.server

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.storage.ServerOperations

class FlushDbCommand(
    private val serverOps: ServerOperations,
) : RedisCommand {
    override val name: String = "FLUSHDB"
    override val arity: Int = 1
    override val flags: List<String> = listOf("write")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        serverOps.flushAll()
        return RESPValue.SimpleString("OK")
    }
}
