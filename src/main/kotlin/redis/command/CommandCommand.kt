package redis.command

import redis.protocol.RESPValue
import redis.protocol.getStringAt

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
        val subCommand = args.getStringAt(1)?.uppercase()

        return when (subCommand) {
            null -> {
                val commands = commandsProvider()
                RESPValue.Array(commands.map { it.toCommandInfo() })
            }
            "COUNT" -> {
                val commands = commandsProvider()
                RESPValue.Integer(commands.size.toLong())
            }
            // TODO: COMMAND DOCS는 각 명령어의 상세 문서를 Map 형태로 반환해야 함
            "DOCS" -> RESPValue.Array(emptyList())
            "INFO" -> {
                val commands = commandsProvider()
                val requestedCommands = args.drop(2)
                    .mapNotNull { (it as? RESPValue.BulkString)?.asString?.uppercase() }
                if (requestedCommands.isEmpty()) {
                    RESPValue.Array(commands.map { it.toCommandInfo() })
                } else {
                    val filtered = commands.filter { it.name.uppercase() in requestedCommands }
                    RESPValue.Array(filtered.map { it.toCommandInfo() })
                }
            }
            else -> RESPValue.Error("ERR unknown subcommand '$subCommand'")
        }
    }
}
