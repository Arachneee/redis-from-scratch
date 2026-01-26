package redis.command

import redis.error.RedisErrors
import redis.protocol.RESPValue

interface RedisCommand {
    val name: String
    val arity: Int
    val flags: List<String>
    val firstKey: Int
    val lastKey: Int
    val step: Int

    fun execute(args: List<RESPValue>): RESPValue

    fun wrongArgsError(): RESPValue.Error = RedisErrors.wrongNumberOfArguments(name.lowercase())

    fun toCommandInfo(): RESPValue.Array =
        RESPValue.Array(
            listOf(
                RESPValue.BulkString(name.lowercase().toByteArray()),
                RESPValue.Integer(arity.toLong()),
                RESPValue.Array(flags.map { RESPValue.BulkString(it.toByteArray()) }),
                RESPValue.Integer(firstKey.toLong()),
                RESPValue.Integer(lastKey.toLong()),
                RESPValue.Integer(step.toLong()),
            ),
        )

    companion object {
        const val FLAG_WRITE = "write"
        const val FLAG_READONLY = "readonly"
        const val FLAG_DENYOOM = "denyoom"
        const val FLAG_FAST = "fast"
        const val FLAG_LOADING = "loading"
        const val FLAG_STALE = "stale"
        const val FLAG_SORT_FOR_SCRIPT = "sort_for_script"
    }
}
