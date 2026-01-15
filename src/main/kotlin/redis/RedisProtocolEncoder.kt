package redis

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import redis.protocol.RESPValue

@Sharable
class RedisProtocolEncoder : MessageToByteEncoder<RESPValue>() {
    override fun encode(
        ctx: ChannelHandlerContext,
        msg: RESPValue,
        out: ByteBuf,
    ) {
        out.writeBytes(msg.toRESP())
    }
}
