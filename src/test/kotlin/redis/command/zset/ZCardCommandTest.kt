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

class ZCardCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var zsetOps: ZSetOperations
    private lateinit var command: ZCardCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        zsetOps = ZSetOperations(store)
        command = ZCardCommand(zsetOps)
    }

    @Test
    fun `ZSet의 멤버 개수를 반환한다`() {
        zsetOps.zadd(
            "myzset",
            listOf(
                1.0 to "a".toByteArray(),
                2.0 to "b".toByteArray(),
                3.0 to "c".toByteArray()
            )
        )

        val args = listOf(
            RESPValue.BulkString("ZCARD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `존재하지 않는 키에 대해 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZCARD".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZCARD".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 zcard하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("ZCARD".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 zcard하면 0을 반환한다`() {
        zsetOps.zadd("myzset", listOf(1.0 to "member1".toByteArray()))
        store.setExpiration("myzset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("ZCARD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }
}
