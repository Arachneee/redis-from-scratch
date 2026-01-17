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

class ZRangeCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var zsetOps: ZSetOperations
    private lateinit var command: ZRangeCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        zsetOps = ZSetOperations(store)
        command = ZRangeCommand(zsetOps)
    }

    @Test
    fun `점수 순서대로 멤버를 반환한다`() {
        zsetOps.zadd(
            "myzset",
            listOf(
                3.0 to "c".toByteArray(),
                1.0 to "a".toByteArray(),
                2.0 to "b".toByteArray()
            )
        )

        val args = listOf(
            RESPValue.BulkString("ZRANGE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(3)
        assertThat(String((array[0] as RESPValue.BulkString).data!!)).isEqualTo("a")
        assertThat(String((array[1] as RESPValue.BulkString).data!!)).isEqualTo("b")
        assertThat(String((array[2] as RESPValue.BulkString).data!!)).isEqualTo("c")
    }

    @Test
    fun `범위를 지정하여 멤버를 반환한다`() {
        zsetOps.zadd(
            "myzset",
            listOf(
                1.0 to "a".toByteArray(),
                2.0 to "b".toByteArray(),
                3.0 to "c".toByteArray()
            )
        )

        val args = listOf(
            RESPValue.BulkString("ZRANGE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(2)
        assertThat(String((array[0] as RESPValue.BulkString).data!!)).isEqualTo("a")
        assertThat(String((array[1] as RESPValue.BulkString).data!!)).isEqualTo("b")
    }

    @Test
    fun `음수 인덱스를 지원한다`() {
        zsetOps.zadd(
            "myzset",
            listOf(
                1.0 to "a".toByteArray(),
                2.0 to "b".toByteArray(),
                3.0 to "c".toByteArray()
            )
        )

        val args = listOf(
            RESPValue.BulkString("ZRANGE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("-2".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(2)
        assertThat(String((array[0] as RESPValue.BulkString).data!!)).isEqualTo("b")
        assertThat(String((array[1] as RESPValue.BulkString).data!!)).isEqualTo("c")
    }

    @Test
    fun `존재하지 않는 키에 대해 빈 배열을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZRANGE".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).isEmpty()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZRANGE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("0".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 zrange하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("ZRANGE".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 zrange하면 빈 배열을 반환한다`() {
        zsetOps.zadd("myzset", listOf(1.0 to "member1".toByteArray()))
        store.setExpiration("myzset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("ZRANGE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).isEmpty()
    }
}
