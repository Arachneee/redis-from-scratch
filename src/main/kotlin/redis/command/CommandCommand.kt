package redis.command

import redis.protocol.RESPValue

class CommandCommand(
    private val commandsProvider: () -> List<RedisCommand>,
) : RedisCommand {
    override val name: String = "COMMAND"
    override val arity: Int = -1
    override val flags: List<String> = listOf("readonly", "loading", "stale")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        val commands = commandsProvider()
        return RESPValue.Array(commands.map { it.toCommandInfo() })
    }
}
