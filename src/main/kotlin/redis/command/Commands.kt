package redis.command

import redis.command.key.DeleteCommand
import redis.command.key.ExistsCommand
import redis.command.key.ExpireCommand
import redis.command.key.KeysCommand
import redis.command.key.PersistCommand
import redis.command.key.PexpireCommand
import redis.command.key.PttlCommand
import redis.command.key.ScanCommand
import redis.command.key.TtlCommand
import redis.command.server.DbSizeCommand
import redis.command.server.EchoCommand
import redis.command.server.FlushDbCommand
import redis.command.server.PingCommand
import redis.command.string.DecrByCommand
import redis.command.string.DecrCommand
import redis.command.string.GetCommand
import redis.command.string.IncrByCommand
import redis.command.string.IncrCommand
import redis.command.string.MGetCommand
import redis.command.string.MSetCommand
import redis.command.string.SetCommand
import redis.command.string.SetNxCommand
import redis.storage.OperationsBundle

fun createCommands(ops: OperationsBundle): List<RedisCommand> =
    listOf(
        GetCommand(ops.string),
        SetCommand(ops.string),
        SetNxCommand(ops.string),
        IncrCommand(ops.string),
        DecrCommand(ops.string),
        IncrByCommand(ops.string),
        DecrByCommand(ops.string),
        MGetCommand(ops.string),
        MSetCommand(ops.string),
        DeleteCommand(ops.key),
        ExistsCommand(ops.key),
        ExpireCommand(ops.key),
        PexpireCommand(ops.key),
        TtlCommand(ops.key),
        PttlCommand(ops.key),
        PersistCommand(ops.key),
        KeysCommand(ops.key),
        ScanCommand(ops.key),
        DbSizeCommand(ops.server),
        FlushDbCommand(ops.server),
        PingCommand(),
        EchoCommand(),
    )
