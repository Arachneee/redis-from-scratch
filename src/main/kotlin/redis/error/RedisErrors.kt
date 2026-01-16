package redis.error

import redis.protocol.RESPValue

object RedisErrors {
    fun unknownCommand(commandName: String?): RESPValue.Error = RESPValue.Error("ERR unknown command '$commandName'")

    fun unknownSubcommand(subCommand: String): RESPValue.Error = RESPValue.Error("ERR unknown subcommand '$subCommand'")

    fun wrongNumberOfArguments(commandName: String): RESPValue.Error =
        RESPValue.Error("ERR wrong number of arguments for '$commandName' command")

    fun syntaxError(): RESPValue.Error = RESPValue.Error("ERR syntax error")

    fun invalidInteger(): RESPValue.Error = RESPValue.Error("ERR value is not an integer or out of range")

    fun wrongType(): RESPValue.Error = RESPValue.Error("WRONGTYPE Operation against a key holding the wrong kind of value")
}
