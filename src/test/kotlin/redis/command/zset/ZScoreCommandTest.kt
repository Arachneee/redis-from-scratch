package redis.command.zset

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.error.WrongTypeException
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.StringOperations
import redis.storage.ZSetOperations

class ZScoreCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var zsetOps: ZSetOperations
    private lateinit var command: ZScoreCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        zsetOps = ZSetOperations(store)
        command = ZScoreCommand(zsetOps)
    }

    @Test
    fun `존재하는 멤버의 점수를 반환한다`() {
        zsetOps.zadd("myzset", listOf(1.5 to "member1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("ZSCORE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).asString).isEqualTo("1.5")
    }

    @Test
    fun `정수 점수는 정수로 포맷된다`() {
        zsetOps.zadd("myzset", listOf(2.0 to "member1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("ZSCORE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).asString).isEqualTo("2")
    }

    @Test
    fun `존재하지 않는 멤버는 null을 반환한다`() {
        zsetOps.zadd("myzset", listOf(1.0 to "member1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("ZSCORE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).data).isNull()
    }

    @Test
    fun `존재하지 않는 키는 null을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZSCORE".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray()),
            RESPValue.BulkString("member".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).data).isNull()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZSCORE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 zscore하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("ZSCORE".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("member".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 zscore하면 null을 반환한다`() {
        zsetOps.zadd("myzset", listOf(1.0 to "member1".toByteArray()))
        store.setExpiration("myzset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("ZSCORE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).data).isNull()
    }
}
