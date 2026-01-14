package redis.command

import redis.RedisRepository

class CommandRegistry(
    repository: RedisRepository,
) {
    private val commands: Map<String, RedisCommand>

    init {
        val baseCommands = listOf(
            GetCommand(repository),
            SetCommand(repository),
            DeleteCommand(repository),
            PingCommand()
        )
        val commandCommand = CommandCommand { commands.values.toList() }
        commands = (baseCommands + commandCommand).associateBy { it.name }
    }

    fun find(name: String): RedisCommand? = commands[name]
}
