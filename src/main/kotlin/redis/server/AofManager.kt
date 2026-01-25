package redis.server

import redis.protocol.RESPValue
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class AofManager(
    filename: String,
) {
    private val fileChannel: FileChannel

    init {
        val file = File(filename)
        fileChannel = FileOutputStream(file, true).channel
    }

    fun append(command: RESPValue) {
        val bytes = command.toRESP()
        val buffer = ByteBuffer.wrap(bytes)
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer)
        }
    }

    fun close() {
        fileChannel.close()
    }
}
