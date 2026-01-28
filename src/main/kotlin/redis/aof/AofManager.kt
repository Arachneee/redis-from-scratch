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
import redis.config.RedisConfig
import redis.protocol.RESPValue
import java.io.BufferedInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.ConcurrentLinkedQueue

class AofManager(
    private val config: RedisConfig,
    private val snapshotProvider: () -> List<RESPValue>,
) {
    private val logger = LoggerFactory.getLogger(AofManager::class.java)
    private val filename = config.aofFilename
    private val file = File(filename)

    private val pendingCommands = ConcurrentLinkedQueue<RESPValue>()
    private var fileChannel: FileChannel = FileOutputStream(file, true).channel
    private val out = ByteArrayOutputStream()
    private val batch = mutableListOf<RESPValue>()
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val mutex = Mutex()
    private lateinit var writerJob: Job

    @Volatile
    private var isRewriting = false

    @Volatile
    private var baseSize: Long = file.length()

    init {
        startWriter()
        startAutoRewriteChecker()
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

    private fun startAutoRewriteChecker() {
        scope.launch {
            while (isActive) {
                try {
                    delay(config.aofCheckIntervalMs)
                    if (isRewriting) continue

                    val currentSize = file.length()
                    val minSize = config.aofRewriteMinSize
                    val percentage = config.aofRewritePercentage

                    val shouldRewrite = currentSize >= minSize &&
                        (baseSize == 0L || (currentSize - baseSize) * 100 / baseSize >= percentage)

                    if (shouldRewrite) {
                        logger.info("Triggering automatic AOF rewrite (current size: $currentSize, base size: $baseSize)")
                        val snapshot = try {
                            snapshotProvider()
                        } catch (e: Exception) {
                            logger.error("Failed to provide snapshot for auto-rewrite", e)
                            null
                        }
                        snapshot?.let { startRewrite(it) }
                    }
                } catch (e: Exception) {
                    logger.error("Error in AOF auto-rewrite checker", e)
                }
            }
        }
    }

    fun append(command: RESPValue) {
        pendingCommands.offer(command)
    }

    private suspend fun flush() {
        mutex.withLock {
            if (isRewriting || pendingCommands.isEmpty()) return@withLock

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

    fun startRewrite(snapshotCommands: List<RESPValue>) {
        if (isRewriting) return
        isRewriting = true
        logger.info("Starting background AOF rewrite")

        scope.launch {
            try {
                val tempFile = File("${file.absolutePath}.temp")
                val tempChannel = FileOutputStream(tempFile).channel

                snapshotCommands.forEach { cmd ->
                    val buf = ByteBuffer.wrap(cmd.toRESP())
                    while (buf.hasRemaining()) {
                        tempChannel.write(buf)
                    }
                }
                tempChannel.force(false)
                tempChannel.close()

                mutex.withLock {
                    fileChannel.close()
                    if (!tempFile.renameTo(file)) {
                        throw IllegalStateException("Failed to rename temp AOF file")
                    }

                    fileChannel = FileOutputStream(file, true).channel
                    baseSize = file.length()
                    isRewriting = false
                }
                logger.info("AOF rewrite completed successfully. New base size: $baseSize")
            } catch (e: Exception) {
                logger.error("AOF rewrite failed", e)
                isRewriting = false
            }
        }
    }
}
