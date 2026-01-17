package redis.command.set

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.error.WrongTypeException
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.SetOperations
import redis.storage.StringOperations

class SAddCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var setOps: SetOperations
    private lateinit var command: SAddCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        setOps = SetOperations(store)
        command = SAddCommand(setOps)
    }

    @Test
    fun `새 Set에 멤버를 추가하면 추가된 개수를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SADD".toByteArray()),
            RESPValue.BulkString("myset".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `여러 멤버를 한번에 추가할 수 있다`() {
        val args = listOf(
            RESPValue.BulkString("SADD".toByteArray()),
            RESPValue.BulkString("myset".toByteArray()),
            RESPValue.BulkString("a".toByteArray()),
            RESPValue.BulkString("b".toByteArray()),
            RESPValue.BulkString("c".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `이미 존재하는 멤버는 추가되지 않는다`() {
        setOps.sadd("myset", listOf("existing".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("SADD".toByteArray()),
            RESPValue.BulkString("myset".toByteArray()),
            RESPValue.BulkString("existing".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SADD".toByteArray()),
            RESPValue.BulkString("myset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 sadd하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("SADD".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("member".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에 sadd하면 새 Set을 생성한다`() {
        setOps.sadd("myset", listOf("old".toByteArray()))
        store.setExpiration("myset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("SADD".toByteArray()),
            RESPValue.BulkString("myset".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }
}
