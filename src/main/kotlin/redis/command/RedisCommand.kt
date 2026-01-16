package redis.command

import redis.protocol.RESPValue

interface RedisCommand {
    val name: String
    val arity: Int
    val flags: List<String>
    val firstKey: Int
    val lastKey: Int
    val step: Int

    fun execute(args: List<RESPValue>): RESPValue

    fun wrongArgsError(): RESPValue.Error =
        RESPValue.Error("ERR wrong number of arguments for '${name.lowercase()}' command")

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
}
