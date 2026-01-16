package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class PersistCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var repository: RedisRepository
    private lateinit var command: PersistCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        repository = RedisRepository(clock)
        command = PersistCommand(repository)
    }

    @Test
    fun `TTL을 제거하고 1을 반환한다`() {
        repository.set("key", "value".toByteArray())
        repository.expire("key", 1)
        val args = listOf(
            RESPValue.BulkString("PERSIST".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1)

        clock.advanceBy(1001)

        assertThat(repository.get("key")).isNotNull
    }

    @Test
    fun `TTL이 없는 키는 0을 반환한다`() {
        repository.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("PERSIST".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
    }

    @Test
    fun `존재하지 않는 키는 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("PERSIST".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("PERSIST".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
