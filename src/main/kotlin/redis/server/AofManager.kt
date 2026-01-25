package redis.server

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import redis.protocol.RESPValue
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class AofManager(
    filename: String,
) {
    private val logger = LoggerFactory.getLogger(AofManager::class.java)
    private val fileChannel: FileChannel
    private val commandChannel = Channel<RESPValue>(Channel.UNLIMITED)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private lateinit var writerJob: Job

    init {
        val file = File(filename)
        fileChannel = FileOutputStream(file, true).channel
        startWriter()
    }

    private fun startWriter() {
        writerJob = scope.launch {
            for (command in commandChannel) {
                try {
                    val bytes = command.toRESP()
                    val buffer = ByteBuffer.wrap(bytes)
                    while (buffer.hasRemaining()) {
                        fileChannel.write(buffer)
                    }
                } catch (e: Exception) {
                    logger.error("Failed to write to AOF", e)
                }
            }
        }
    }

    fun append(command: RESPValue) {
        commandChannel.trySend(command)
    }

    fun close() {
        commandChannel.close()
        runBlocking { writerJob.join() }
        fileChannel.close()
    }
}
