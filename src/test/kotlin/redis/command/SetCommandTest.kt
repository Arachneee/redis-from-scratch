package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class SetCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var repository: RedisRepository
    private lateinit var command: SetCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        repository = RedisRepository(clock)
        command = SetCommand(repository)
    }

    @Test
    fun `키와 값을 저장하고 OK를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SET".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("value".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
        assertThat(repository.get("key")?.let { String(it) }).isEqualTo("value")
    }

    @Test
    fun `EX 옵션으로 초 단위 TTL을 설정한다`() {
        val args = listOf(
            RESPValue.BulkString("SET".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("value".toByteArray()),
            RESPValue.BulkString("EX".toByteArray()),
            RESPValue.BulkString("5".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat(repository.get("key")).isNotNull

        clock.advanceBy(5001)

        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `PX 옵션으로 밀리초 단위 TTL을 설정한다`() {
        val args = listOf(
            RESPValue.BulkString("SET".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("value".toByteArray()),
            RESPValue.BulkString("PX".toByteArray()),
            RESPValue.BulkString("500".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat(repository.get("key")).isNotNull

        clock.advanceBy(501)

        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `ex 옵션은 대소문자를 구분하지 않는다`() {
        val args = listOf(
            RESPValue.BulkString("SET".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("value".toByteArray()),
            RESPValue.BulkString("ex".toByteArray()),
            RESPValue.BulkString("5".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SET".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `TTL 값이 0 이하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SET".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("value".toByteArray()),
            RESPValue.BulkString("EX".toByteArray()),
            RESPValue.BulkString("0".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `잘못된 옵션은 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SET".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("value".toByteArray()),
            RESPValue.BulkString("INVALID".toByteArray()),
            RESPValue.BulkString("5".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
