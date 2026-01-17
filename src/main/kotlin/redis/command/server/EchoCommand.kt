package redis.command.server

import redis.command.RedisCommand
import redis.protocol.RESPValue

class EchoCommand : RedisCommand {
    override val name: String = "ECHO"
    override val arity: Int = 2
    override val flags: List<String> = listOf("fast")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size < 2) return wrongArgsError()
        return when (val message = args[1]) {
            is RESPValue.BulkString -> message
            else -> wrongArgsError()
        }
    }
}
