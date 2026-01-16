package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class FlushDbCommandTest {
    private lateinit var repository: RedisRepository
    private lateinit var command: FlushDbCommand

    @BeforeEach
    fun setUp() {
        repository = RedisRepository(FakeClock())
        command = FlushDbCommand(repository)
    }

    @Test
    fun `모든 키를 삭제하고 OK를 반환한다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())
        val args = listOf(RESPValue.BulkString("FLUSHDB".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
        assertThat(repository.size()).isEqualTo(0)
    }

    @Test
    fun `비어있어도 OK를 반환한다`() {
        val args = listOf(RESPValue.BulkString("FLUSHDB".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
    }
}
