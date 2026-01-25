package redis.server

import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPromise
import org.slf4j.LoggerFactory
import redis.protocol.RESPValue
import redis.command.CommandRegistry
import java.util.ArrayDeque

class AofHandler(
    private val commandRegistry: CommandRegistry,
) : ChannelDuplexHandler() {
    private val logger = LoggerFactory.getLogger(AofHandler::class.java)
    private val commandQueue = ArrayDeque<RESPValue>()

    override fun channelRead(
        ctx: ChannelHandlerContext,
        msg: Any,
    ) {
        if (msg is RESPValue) {
            commandQueue.add(msg)
            ctx.fireChannelRead(msg)
        } else {
            ctx.fireChannelRead(msg)
        }
    }

    override fun write(
        ctx: ChannelHandlerContext,
        msg: Any,
        promise: ChannelPromise,
    ) {
        if (msg is RESPValue) {
            val command = commandQueue.poll()
            val commandName = (command as? RESPValue.Array)?.getCommand() ?: ""

            val isReadOnly = commandRegistry.find(commandName)?.flags?.contains("readonly") == true

            if (msg !is RESPValue.Error && command != null && !isReadOnly) {
                logger.info("[AOF] Command executed: \n{}", String(command.toRESP(), Charsets.UTF_8))
            }
        }
        ctx.write(msg, promise)
    }
}
