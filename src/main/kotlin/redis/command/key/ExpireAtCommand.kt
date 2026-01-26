package redis.command.key

import redis.command.RedisCommand
import redis.protocol.RESPValue
import redis.protocol.getStringAt
import redis.storage.KeyOperations

class ExpireAtCommand(
    private val keyOps: KeyOperations,
) : RedisCommand {
    override val name: String = NAME
    override val arity: Int = -3
    override val flags: List<String> = listOf("write", "fast")
    override val firstKey: Int = 1
    override val lastKey: Int = 1
    override val step: Int = 1

    override fun execute(args: List<RESPValue>): RESPValue {
        val key = args.getStringAt(1) ?: return wrongArgsError()
        val timestamp = args.getStringAt(2)?.toLongOrNull() ?: return wrongArgsError()

        // NX, XX, GT, LT 옵션 처리 (필요시)
        // 현재 KeyOperations가 기본 만료 설정만 지원하므로 기본 동작 수행
        val count = keyOps.expireAt(key, timestamp * 1000)
        return RESPValue.Integer(count)
    }

    companion object {
        const val NAME = "EXPIREAT"
    }
}
