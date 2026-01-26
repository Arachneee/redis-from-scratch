package redis.command.key

import redis.command.RedisCommand
import redis.error.RedisErrors
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations

class ScanCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = "SCAN"
    override val arity: Int = -2
    override val flags: List<String> = listOf(RedisCommand.FLAG_READONLY)
    override val firstKey: Int = 0
    override val lastKey: Int = 0
    override val step: Int = 0

    override fun execute(args: List<RESPValue>): RESPValue {
        val cursor = args.getStringAt(1)?.toLongOrNull() ?: return RedisErrors.invalidInteger()

        var pattern: String? = null
        var count = DEFAULT_COUNT

        var i = 2
        while (i < args.size) {
            val option = args.getStringAt(i)?.uppercase()
            when (option) {
                "MATCH" -> {
                    pattern = args.getStringAt(i + 1) ?: return RedisErrors.syntaxError()
                    i += 2
                }
                "COUNT" -> {
                    count = args.getStringAt(i + 1)?.toIntOrNull() ?: return RedisErrors.invalidInteger()
                    i += 2
                }
                else -> return RedisErrors.syntaxError()
            }
        }

        val (nextCursor, keys) = keyOps.scan(cursor, pattern, count)

        return RESPValue.Array(
            listOf(
                RESPValue.BulkString(nextCursor.toString().toByteArray()),
                RESPValue.Array(keys.map { RESPValue.BulkString(it.toByteArray()) })
            )
        )
    }

    companion object {
        private const val DEFAULT_COUNT = 10
    }
}
