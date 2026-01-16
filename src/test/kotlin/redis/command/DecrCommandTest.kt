package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class DecrCommandTest {
    private lateinit var repository: RedisRepository
    private lateinit var command: DecrCommand

    @BeforeEach
    fun setUp() {
        repository = RedisRepository(FakeClock())
        command = DecrCommand(repository)
    }

    @Test
    fun `존재하지 않는 키를 -1로 설정하고 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("DECR".toByteArray()),
            RESPValue.BulkString("counter".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(-1)
    }

    @Test
    fun `기존 숫자 값을 1 감소시킨다`() {
        repository.set("counter", "10".toByteArray())
        val args = listOf(
            RESPValue.BulkString("DECR".toByteArray()),
            RESPValue.BulkString("counter".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(9)
    }

    @Test
    fun `숫자가 아닌 값에 대해 에러를 반환한다`() {
        repository.set("key", "not-a-number".toByteArray())
        val args = listOf(
            RESPValue.BulkString("DECR".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("DECR")
        assertThat(command.arity).isEqualTo(2)
    }
}
