package redis.aof

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.storage.AofOperations
import redis.storage.SnapshotValue

class BGRewriteAofCommand(
    private val aofOperations: AofOperations,
    private val aofManager: AofManager,
) : RedisCommand {
    override val name: String = NAME
    override val arity: Int = 1
    override val flags: List<String> = listOf("admin", "noscript")
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        if (args.size != 1) {
            return wrongArgsError()
        }

        val rewriteCommands = AofRewriteService.generateRewriteCommands(aofOperations)
        aofManager.startRewrite(rewriteCommands)

        return RESPValue.SimpleString("Background append only file rewriting started")
    }

    companion object {
        const val NAME = "BGREWRITEAOF"
    }
}
