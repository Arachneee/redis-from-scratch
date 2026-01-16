package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class PexpireCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var repository: RedisRepository
    private lateinit var command: PexpireCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        repository = RedisRepository(clock)
        command = PexpireCommand(repository)
    }

    @Test
    fun `밀리초 단위로 TTL을 설정하고 1을 반환한다`() {
        repository.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("PEXPIRE".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("500".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(1)
        assertThat(repository.get("key")).isNotNull

        clock.advanceBy(501)

        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `존재하지 않는 키는 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("PEXPIRE".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray()),
            RESPValue.BulkString("1000".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
    }

    @Test
    fun `0 이하 값이면 키를 삭제한다`() {
        repository.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("PEXPIRE".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("0".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(1)
        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("PEXPIRE".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("PEXPIRE")
        assertThat(command.arity).isEqualTo(3)
        assertThat(command.flags).contains("write", "fast")
    }
}
