package redis.command

import redis.command.server.CommandCommand
import redis.storage.OperationsBundle

class CommandRegistry(
    ops: OperationsBundle,
) {
    private val commands: Map<String, RedisCommand>

    init {
        val baseCommands = createCommands(ops)
        val commandCommand = CommandCommand { commands.values.toList() }
        commands = (baseCommands + commandCommand).associateBy { it.name }
    }

    fun find(name: String): RedisCommand? = commands[name]
}
