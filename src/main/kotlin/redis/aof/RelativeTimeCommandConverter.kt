package redis.aof

import redis.command.key.ExpireAtCommand
import redis.command.key.ExpireCommand
import redis.command.key.PexpireAtCommand
import redis.command.key.PexpireCommand
import redis.command.string.GetExCommand
import redis.command.string.PSetExCommand
import redis.command.string.SetCommand
import redis.command.string.SetExCommand
import redis.protocol.RESPValue

object RelativeTimeCommandConverter {
    private val RESP_EXPIREAT = RESPValue.BulkString(ExpireAtCommand.NAME.toByteArray())
    private val RESP_PEXPIREAT = RESPValue.BulkString(PexpireAtCommand.NAME.toByteArray())
    private val RESP_SET = RESPValue.BulkString(SetCommand.NAME.toByteArray())
    private val RESP_PXAT = RESPValue.BulkString(SetCommand.OPTION_PXAT.toByteArray())

    fun transform(command: RESPValue): RESPValue {
        val commandArray = command as? RESPValue.Array ?: return command
        val elements = commandArray.elements
        if (elements.isNullOrEmpty()) {
            return command
        }

        val commandName = (elements.first() as? RESPValue.BulkString)?.asString?.uppercase() ?: return command

        return when (commandName) {
            ExpireCommand.NAME -> toAbsoluteTimeExpire(command, elements, isSeconds = true)
            PexpireCommand.NAME -> toAbsoluteTimeExpire(command, elements, isSeconds = false)
            SetExCommand.NAME -> transformSetExStyle(command, elements, isSeconds = true)
            PSetExCommand.NAME -> transformSetExStyle(command, elements, isSeconds = false)
            SetCommand.NAME -> transformSet(command, elements)
            GetExCommand.NAME -> transformGetEx(command, elements)
            else -> command
        }
    }

    private fun toAbsoluteTimeExpire(
        originalCommand: RESPValue,
        elements: List<RESPValue>,
        isSeconds: Boolean,
    ): RESPValue {
        if (elements.size != 3 || elements.any { it !is RESPValue.BulkString }) return originalCommand

        val key = elements[1] as RESPValue.BulkString
        val time = (elements[2] as RESPValue.BulkString).asString?.toLongOrNull() ?: return originalCommand

        val now = System.currentTimeMillis()
        val expireAt = if (isSeconds) (now / 1000) + time else now + time

        val newCommand = if (isSeconds) RESP_EXPIREAT else RESP_PEXPIREAT

        return RESPValue.Array(
            listOf(
                newCommand,
                key,
                RESPValue.BulkString(expireAt.toString().toByteArray()),
            ),
        )
    }

    private fun transformSetExStyle(
        originalCommand: RESPValue,
        elements: List<RESPValue>,
        isSeconds: Boolean,
    ): RESPValue {
        if (elements.size != 4 || elements.any { it !is RESPValue.BulkString }) return originalCommand
        val key = elements[1] as RESPValue.BulkString
        val time = (elements[2] as RESPValue.BulkString).asString?.toLongOrNull() ?: return originalCommand
        val value = elements[3] as RESPValue.BulkString
        val multiplier = if (isSeconds) 1000L else 1L
        val pexpireAt = System.currentTimeMillis() + time * multiplier
        return RESPValue.Array(
            listOf(
                RESP_SET,
                key,
                value,
                RESP_PXAT,
                RESPValue.BulkString(pexpireAt.toString().toByteArray()),
            ),
        )
    }

    private fun transformSet(
        originalCommand: RESPValue,
        elements: List<RESPValue>,
    ): RESPValue {
        val stringElements = elements.mapNotNull { (it as? RESPValue.BulkString)?.asString?.uppercase() }

        return applyExpiryOptionConversion(originalCommand, elements, stringElements, SetCommand.OPTION_EX, 1000, 2)
            ?: applyExpiryOptionConversion(originalCommand, elements, stringElements, SetCommand.OPTION_PX, 1, 2)
            ?: originalCommand
    }

    private fun transformGetEx(
        originalCommand: RESPValue,
        elements: List<RESPValue>,
    ): RESPValue {
        val stringElements = elements.mapNotNull { (it as? RESPValue.BulkString)?.asString?.uppercase() }

        return applyExpiryOptionConversion(originalCommand, elements, stringElements, SetCommand.OPTION_EX, 1000, 1)
            ?: applyExpiryOptionConversion(originalCommand, elements, stringElements, SetCommand.OPTION_PX, 1, 1)
            ?: originalCommand
    }

    private fun applyExpiryOptionConversion(
        originalCommand: RESPValue,
        elements: List<RESPValue>,
        stringElements: List<String>,
        option: String,
        multiplier: Long,
        minIndex: Int,
    ): RESPValue? {
        val index = stringElements.indexOf(option)
        if (index > minIndex && index + 1 < stringElements.size) {
            val time =
                (elements[index + 1] as? RESPValue.BulkString)?.asString?.toLongOrNull()
                    ?: return originalCommand

            val pexpireAt = System.currentTimeMillis() + time * multiplier
            val newElements = elements.toMutableList()
            newElements[index] = RESP_PXAT
            newElements[index + 1] = RESPValue.BulkString(pexpireAt.toString().toByteArray())
            return RESPValue.Array(newElements)
        }
        return null
    }
}
