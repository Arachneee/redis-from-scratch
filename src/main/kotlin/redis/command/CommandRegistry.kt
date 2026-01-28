package redis.command

import redis.aof.AofManager
import redis.command.server.CommandCommand
import redis.storage.OperationsBundle

class CommandRegistry(
    ops: OperationsBundle,
    aofManager: AofManager,
) {
    private val commands: Map<String, RedisCommand>

    init {
        val baseCommands = createCommands(ops, aofManager)
        val commandCommand = CommandCommand { commands.values.toList() }
        commands = (baseCommands + commandCommand).associateBy { it.name }
    }

    fun find(name: String): RedisCommand? = commands[name]
}
