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

class SMembersCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var setOps: SetOperations
    private lateinit var command: SMembersCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        setOps = SetOperations(store)
        command = SMembersCommand(setOps)
    }

    @Test
    fun `Set의 모든 멤버를 반환한다`() {
        setOps.sadd("myset", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("SMEMBERS".toByteArray()),
            RESPValue.BulkString("myset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(3)

        val members = array.map { String((it as RESPValue.BulkString).data!!) }.toSet()
        assertThat(members).containsExactlyInAnyOrder("a", "b", "c")
    }

    @Test
    fun `존재하지 않는 키에 대해 빈 배열을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SMEMBERS".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).isEmpty()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SMEMBERS".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 smembers하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("SMEMBERS".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 smembers하면 빈 배열을 반환한다`() {
        setOps.sadd("myset", listOf("member1".toByteArray()))
        store.setExpiration("myset", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("SMEMBERS".toByteArray()),
            RESPValue.BulkString("myset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).isEmpty()
    }
}
