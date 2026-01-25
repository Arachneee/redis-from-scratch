package redis.server

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.slf4j.LoggerFactory
import redis.command.CommandRegistry
import redis.error.RedisErrors
import redis.error.WrongTypeException
import redis.protocol.RESPValue

@Sharable
class RedisCommandHandler(
    private val commandRegistry: CommandRegistry,
) : SimpleChannelInboundHandler<RESPValue>() {
    private val logger = LoggerFactory.getLogger(RedisCommandHandler::class.java)

    override fun channelRead0(
        ctx: ChannelHandlerContext,
        msg: RESPValue,
    ) {
        if (msg is RESPValue.Array) {
            val commandName = msg.getCommand()
            val response =
                try {
                    commandName
                        ?.let { commandRegistry.find(it) }
                        ?.execute(msg.elements ?: emptyList())
                        ?: RedisErrors.unknownCommand(commandName)
                } catch (e: WrongTypeException) {
                    RedisErrors.wrongType()
                }

            ctx.writeAndFlush(response)
        }
    }

    override fun exceptionCaught(
        ctx: ChannelHandlerContext,
        cause: Throwable,
    ) {
        logger.error("Exception caught in channel handler", cause)
        ctx.close()
    }
}
