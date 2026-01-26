package redis.command.server

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.storage.ServerOperations

class DbSizeCommand(
    private val serverOps: ServerOperations,
) : RedisCommand {
    override val name: String = "DBSIZE"
    override val arity: Int = 1
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY, RedisCommand.FLAG_FAST)
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        return RESPValue.Integer(serverOps.size())
    }
}
