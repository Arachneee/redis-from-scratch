package redis.aof

import redis.protocol.RESPValue
import redis.storage.AofOperations
import redis.storage.SnapshotValue

object AofRewriteService {
    fun generateRewriteCommands(aofOperations: AofOperations): List<RESPValue> {
        val state = aofOperations.captureState()
        val rewriteCommands = mutableListOf<RESPValue>()

        state.forEach { (key, value) ->
            val keyBytes = key.toByteArray()

            when (value) {
                is SnapshotValue.String -> {
                    rewriteCommands.add(
                        RESPValue.Array(
                            listOf(
                                RESPValue.BulkString("SET".toByteArray()),
                                RESPValue.BulkString(keyBytes),
                                RESPValue.BulkString(value.value),
                            ),
                        ),
                    )
                }

                is SnapshotValue.List -> {
                    val cmdElements = mutableListOf<RESPValue>()
                    cmdElements.add(RESPValue.BulkString("RPUSH".toByteArray()))
                    cmdElements.add(RESPValue.BulkString(keyBytes))
                    value.elements.forEach { cmdElements.add(RESPValue.BulkString(it)) }
                    rewriteCommands.add(RESPValue.Array(cmdElements))
                }

                is SnapshotValue.Set -> {
                    val cmdElements = mutableListOf<RESPValue>()
                    cmdElements.add(RESPValue.BulkString("SADD".toByteArray()))
                    cmdElements.add(RESPValue.BulkString(keyBytes))
                    value.members.forEach { cmdElements.add(RESPValue.BulkString(it)) }
                    rewriteCommands.add(RESPValue.Array(cmdElements))
                }

                is SnapshotValue.Hash -> {
                    val cmdElements = mutableListOf<RESPValue>()
                    cmdElements.add(RESPValue.BulkString("HSET".toByteArray()))
                    cmdElements.add(RESPValue.BulkString(keyBytes))
                    value.fields.forEach { (field, fValue) ->
                        cmdElements.add(RESPValue.BulkString(field.toByteArray()))
                        cmdElements.add(RESPValue.BulkString(fValue))
                    }
                    rewriteCommands.add(RESPValue.Array(cmdElements))
                }

                is SnapshotValue.ZSet -> {
                    val cmdElements = mutableListOf<RESPValue>()
                    cmdElements.add(RESPValue.BulkString("ZADD".toByteArray()))
                    cmdElements.add(RESPValue.BulkString(keyBytes))
                    value.members.forEach { (member, score) ->
                        cmdElements.add(RESPValue.BulkString(score.toString().toByteArray()))
                        cmdElements.add(RESPValue.BulkString(member))
                    }
                    rewriteCommands.add(RESPValue.Array(cmdElements))
                }
            }

            aofOperations.getExpiration(key)?.let { expireAt ->
                rewriteCommands.add(
                    RESPValue.Array(
                        listOf(
                            RESPValue.BulkString("PEXPIREAT".toByteArray()),
                            RESPValue.BulkString(keyBytes),
                            RESPValue.BulkString(expireAt.toString().toByteArray()),
                        ),
                    ),
                )
            }
        }

        return rewriteCommands
    }
}
