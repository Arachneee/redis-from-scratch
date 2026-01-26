package redis.aof

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import redis.protocol.RESPValue
import java.io.BufferedInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.ConcurrentLinkedQueue

class AofManager(
    filename: String,
) {
    private val logger = LoggerFactory.getLogger(AofManager::class.java)
    private val file = File(filename)

    private val pendingCommands = ConcurrentLinkedQueue<RESPValue>()
    private val fileChannel: FileChannel
    private val out = ByteArrayOutputStream()
    private val batch = mutableListOf<RESPValue>()
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val mutex = Mutex()
    private lateinit var writerJob: Job

    init {
        fileChannel = FileOutputStream(file, true).channel
        startWriter()
    }

    private fun startWriter() {
        writerJob =
            scope.launch {
                while (isActive) {
                    try {
                        delay(1000)
                        flush()
                    } catch (e: Exception) {
                        logger.error("Failed to write to AOF", e)
                    }
                }
            }
    }

    fun append(command: RESPValue) {
        pendingCommands.offer(command)
    }

    private suspend fun flush() {
        mutex.withLock {
            if (pendingCommands.isEmpty()) return@withLock

            batch.clear()
            while (pendingCommands.isNotEmpty()) {
                pendingCommands.poll()?.let { batch.add(it) }
            }

            try {
                out.reset()
                batch.forEach { command ->
                    out.write(command.toRESP())
                }

                val buffer = ByteBuffer.wrap(out.toByteArray())

                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer)
                }
                fileChannel.force(false)
            } catch (e: Exception) {
                logger.error("AOF write failed. Re-queueing commands.", e)
                batch.reversed().forEach { pendingCommands.offer(it) }
            }
        }
    }

    fun close() {
        runBlocking {
            writerJob.cancel()
            writerJob.join()
            flush()
        }
        fileChannel.close()
    }

    fun readAof(): BufferedInputStream? {
        if (!file.exists() || file.length() == 0L) return null
        return file.inputStream().buffered()
    }
}
