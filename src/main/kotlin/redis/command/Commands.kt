package redis.command

import redis.storage.RedisRepository

fun createCommands(repository: RedisRepository): List<RedisCommand> =
    listOf(
        GetCommand(repository),
        SetCommand(repository),
        DeleteCommand(repository),
        ExpireCommand(repository),
        TtlCommand(repository),
        PingCommand(),
    )
