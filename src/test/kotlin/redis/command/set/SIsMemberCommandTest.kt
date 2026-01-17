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

class SIsMemberCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var setOps: SetOperations
    private lateinit var command: SIsMemberCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        setOps = SetOperations(store)
        command = SIsMemberCommand(setOps)
    }

    @Test
    fun `존재하는 멤버에 대해 1을 반환한다`() {
        setOps.sadd("myset", listOf("member1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("SISMEMBER".toByteArray()),
            RESPValue.BulkString("myset".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `존재하지 않는 멤버에 대해 0을 반환한다`() {
        setOps.sadd("myset", listOf("member1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("SISMEMBER".toByteArray()),
            RESPValue.BulkString("myset".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `존재하지 않는 키에 대해 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SISMEMBER".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray()),
            RESPValue.BulkString("member".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SISMEMBER".toByteArray()),
            RESPValue.BulkString("myset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 sismember하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("SISMEMBER".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("member".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 sismember하면 0을 반환한다`() {
        setOps.sadd("myset", listOf("member1".toByteArray()))
        store.setExpiration("myset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("SISMEMBER".toByteArray()),
            RESPValue.BulkString("myset".toByteArray()),
            RESPValue.BulkString("member1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }
}
