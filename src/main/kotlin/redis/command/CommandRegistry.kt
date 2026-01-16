package redis.command

import redis.storage.RedisRepository

class CommandRegistry(
    repository: RedisRepository,
) {
    private val commands: Map<String, RedisCommand>

    init {
        val baseCommands = createCommands(repository)
        val commandCommand = CommandCommand { commands.values.toList() }
        commands = (baseCommands + commandCommand).associateBy { it.name }
    }

    fun find(name: String): RedisCommand? = commands[name]
}
