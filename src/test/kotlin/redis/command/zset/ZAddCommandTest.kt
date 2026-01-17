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

class ZAddCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var zsetOps: ZSetOperations
    private lateinit var command: ZAddCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        zsetOps = ZSetOperations(store)
        command = ZAddCommand(zsetOps)
    }

    @Test
    fun `새 ZSet에 멤버를 추가하면 추가된 개수를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZADD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("1".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `여러 멤버를 한번에 추가할 수 있다`() {
        val args = listOf(
            RESPValue.BulkString("ZADD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("1".toByteArray()),
            RESPValue.BulkString("a".toByteArray()),
            RESPValue.BulkString("2".toByteArray()),
            RESPValue.BulkString("b".toByteArray()),
            RESPValue.BulkString("3".toByteArray()),
            RESPValue.BulkString("c".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `이미 존재하는 멤버의 점수를 업데이트하면 0을 반환한다`() {
        zsetOps.zadd("myzset", listOf(1.0 to "member1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("ZADD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("2".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)

        val score = zsetOps.zscore("myzset", "member1".toByteArray())
        assertThat(score).isEqualTo(2.0)
    }

    @Test
    fun `유효하지 않은 점수는 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZADD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("notanumber".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ZADD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 zadd하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("ZADD".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("1".toByteArray()),
            RESPValue.BulkString("member".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에 zadd하면 새 ZSet을 생성한다`() {
        zsetOps.zadd("myzset", listOf(1.0 to "old".toByteArray()))
        store.setExpiration("myzset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("ZADD".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray()),
            RESPValue.BulkString("1".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }
}
