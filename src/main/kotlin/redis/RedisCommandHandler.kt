package redis

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import redis.protocol.RESPValue

class RedisCommandHandler : SimpleChannelInboundHandler<RESPValue>() {
    override fun channelRead0(
        ctx: ChannelHandlerContext,
        msg: RESPValue,
    ) {
        println("Received: $msg")

        if (msg is RESPValue.Array) {
            val command = (msg.elements?.firstOrNull() as? RESPValue.BulkString)?.value?.uppercase()
            val response = when (command) {
                "COMMAND" -> RESPValue.Array(emptyList())
                "PING" -> RESPValue.SimpleString("PONG")
                else -> RESPValue.Error("ERR unknown command '$command'")
            }
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
