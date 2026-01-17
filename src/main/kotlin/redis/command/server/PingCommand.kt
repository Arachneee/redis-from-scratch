package redis.command.server

import redis.command.RedisCommand
import redis.protocol.RESPValue

class PingCommand : RedisCommand {
    override val name: String = "PING"
    override val arity: Int = 1
    override val flags: List<String> = listOf("fast")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        return RESPValue.SimpleString("PONG")
    }
}
