package redis

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import redis.protocol.RESPValue

class RedisProtocolEncoder : MessageToByteEncoder<RESPValue>() {
    override fun encode(
        ctx: ChannelHandlerContext,
        msg: RESPValue,
        out: ByteBuf,
    ) {
        out.writeBytes(msg.toRESP().toByteArray(Charsets.UTF_8))
    }
}
