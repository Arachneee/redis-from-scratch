package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class DbSizeCommandTest {
    private lateinit var repository: RedisRepository
    private lateinit var command: DbSizeCommand

    @BeforeEach
    fun setUp() {
        repository = RedisRepository(FakeClock())
        command = DbSizeCommand(repository)
    }

    @Test
    fun `저장된 키의 개수를 반환한다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())
        repository.set("key3", "value3".toByteArray())
        val args = listOf(RESPValue.BulkString("DBSIZE".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3)
    }

    @Test
    fun `비어있으면 0을 반환한다`() {
        val args = listOf(RESPValue.BulkString("DBSIZE".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
    }
}
