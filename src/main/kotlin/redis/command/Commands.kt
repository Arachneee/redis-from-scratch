package redis.command

import redis.storage.RedisRepository

fun createCommands(repository: RedisRepository): List<RedisCommand> =
    listOf(
        GetCommand(repository),
        SetCommand(repository),
        DeleteCommand(repository),
        ExpireCommand(repository),
        TtlCommand(repository),
        ExistsCommand(repository),
        DbSizeCommand(repository),
        FlushDbCommand(repository),
        PersistCommand(repository),
        PingCommand(),
        EchoCommand(),
        IncrCommand(repository),
        DecrCommand(repository),
        IncrByCommand(repository),
        DecrByCommand(repository),
        SetNxCommand(repository),
        MGetCommand(repository),
        MSetCommand(repository),
        PttlCommand(repository),
        PexpireCommand(repository),
        KeysCommand(repository),
        ScanCommand(repository),
    )
