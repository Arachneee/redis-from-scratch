package redis.command

import redis.command.hash.HDelCommand
import redis.command.hash.HExistsCommand
import redis.command.hash.HGetAllCommand
import redis.command.hash.HGetCommand
import redis.command.hash.HLenCommand
import redis.command.hash.HSetCommand
import redis.command.key.DeleteCommand
import redis.command.key.RenameCommand
import redis.command.key.TypeCommand
import redis.command.set.SAddCommand
import redis.command.set.SCardCommand
import redis.command.set.SIsMemberCommand
import redis.command.set.SMembersCommand
import redis.command.set.SRemCommand
import redis.command.zset.ZAddCommand
import redis.command.zset.ZCardCommand
import redis.command.zset.ZRangeCommand
import redis.command.zset.ZRankCommand
import redis.command.zset.ZRemCommand
import redis.command.zset.ZScoreCommand
import redis.command.key.ExistsCommand
import redis.command.key.ExpireCommand
import redis.command.key.KeysCommand
import redis.command.key.PersistCommand
import redis.command.key.PexpireCommand
import redis.command.key.PttlCommand
import redis.command.key.ScanCommand
import redis.command.key.TtlCommand
import redis.command.list.LLenCommand
import redis.command.list.LPopCommand
import redis.command.list.LPushCommand
import redis.command.list.LRangeCommand
import redis.command.list.RPopCommand
import redis.command.list.RPushCommand
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
import redis.command.string.SetExCommand
import redis.command.string.SetNxCommand
import redis.command.string.AppendCommand
import redis.command.string.StrlenCommand
import redis.storage.OperationsBundle

fun createCommands(ops: OperationsBundle): List<RedisCommand> =
    listOf(
        GetCommand(ops.string),
        SetCommand(ops.string),
        SetNxCommand(ops.string),
        SetExCommand(ops.string),
        AppendCommand(ops.string),
        StrlenCommand(ops.string),
        IncrCommand(ops.string),
        DecrCommand(ops.string),
        IncrByCommand(ops.string),
        DecrByCommand(ops.string),
        MGetCommand(ops.string),
        MSetCommand(ops.string),
        DeleteCommand(ops.key),
        TypeCommand(ops.key),
        RenameCommand(ops.key),
        ExistsCommand(ops.key),
        ExpireCommand(ops.key),
        PexpireCommand(ops.key),
        TtlCommand(ops.key),
        PttlCommand(ops.key),
        PersistCommand(ops.key),
        KeysCommand(ops.key),
        ScanCommand(ops.key),
        LPushCommand(ops.list),
        RPushCommand(ops.list),
        LPopCommand(ops.list),
        RPopCommand(ops.list),
        LRangeCommand(ops.list),
        LLenCommand(ops.list),
        HSetCommand(ops.hash),
        HGetCommand(ops.hash),
        HDelCommand(ops.hash),
        HExistsCommand(ops.hash),
        HLenCommand(ops.hash),
        HGetAllCommand(ops.hash),
        SAddCommand(ops.set),
        SRemCommand(ops.set),
        SMembersCommand(ops.set),
        SIsMemberCommand(ops.set),
        SCardCommand(ops.set),
        ZAddCommand(ops.zset),
        ZRemCommand(ops.zset),
        ZScoreCommand(ops.zset),
        ZRankCommand(ops.zset),
        ZRangeCommand(ops.zset),
        ZCardCommand(ops.zset),
        DbSizeCommand(ops.server),
        FlushDbCommand(ops.server),
        PingCommand(),
        EchoCommand(),
    )
