package redis

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import redis.command.CommandRegistry
import redis.protocol.RESPValue

class RedisCommandHandler(
    private val commandRegistry: CommandRegistry,
) : SimpleChannelInboundHandler<RESPValue>() {
    override fun channelRead0(
        ctx: ChannelHandlerContext,
        msg: RESPValue,
    ) {
        println("Received: $msg")

        if (msg is RESPValue.Array) {
            val commandName = msg.getCommand()
            val response =
                commandName
                    ?.let { commandRegistry.find(it) }
                    ?.execute(msg.elements ?: emptyList())
                    ?: RESPValue.Error("ERR unknown command '$commandName'")
            ctx.writeAndFlush(response)
        }
    }

    override fun exceptionCaught(
        ctx: ChannelHandlerContext,
        cause: Throwable,
    ) {
        cause.printStackTrace()
        ctx.close()
    }
}
