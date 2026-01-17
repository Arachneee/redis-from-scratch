package redis.command.hash

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.error.WrongTypeException
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.HashOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class HLenCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var hashOps: HashOperations
    private lateinit var command: HLenCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        hashOps = HashOperations(store)
        command = HLenCommand(hashOps)
    }

    @Test
    fun `해시의 필드 개수를 반환한다`() {
        hashOps.hset(
            "myhash",
            listOf(
                "field1" to "value1".toByteArray(),
                "field2" to "value2".toByteArray(),
                "field3" to "value3".toByteArray()
            )
        )

        val args = listOf(
            RESPValue.BulkString("HLEN".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `빈 해시는 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HLEN".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HLEN".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 hlen하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("HLEN".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 hlen하면 0을 반환한다`() {
        hashOps.hset("myhash", listOf("field1" to "value1".toByteArray()))
        store.setExpiration("myhash", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("HLEN".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }
}
