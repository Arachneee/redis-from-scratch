package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class GetCommandTest {
    private lateinit var repository: RedisRepository
    private lateinit var command: GetCommand

    @BeforeEach
    fun setUp() {
        repository = RedisRepository(FakeClock())
        command = GetCommand(repository)
    }

    @Test
    fun `존재하는 키의 값을 반환한다`() {
        repository.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("GET".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).asString).isEqualTo("value")
    }

    @Test
    fun `존재하지 않는 키는 null BulkString을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("GET".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).data).isNull()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("GET".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("GET")
        assertThat(command.arity).isEqualTo(2)
        assertThat(command.flags).contains("readonly", "fast")
    }
}
