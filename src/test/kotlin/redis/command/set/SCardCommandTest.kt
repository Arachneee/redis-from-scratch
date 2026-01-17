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

class SCardCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var setOps: SetOperations
    private lateinit var command: SCardCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        setOps = SetOperations(store)
        command = SCardCommand(setOps)
    }

    @Test
    fun `Set의 멤버 개수를 반환한다`() {
        setOps.sadd("myset", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("SCARD".toByteArray()),
            RESPValue.BulkString("myset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `존재하지 않는 키에 대해 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SCARD".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SCARD".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 scard하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("SCARD".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 scard하면 0을 반환한다`() {
        setOps.sadd("myset", listOf("member1".toByteArray()))
        store.setExpiration("myset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("SCARD".toByteArray()),
            RESPValue.BulkString("myset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }
}
