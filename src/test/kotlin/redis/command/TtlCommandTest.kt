package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class TtlCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var repository: RedisRepository
    private lateinit var command: TtlCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        repository = RedisRepository(clock)
        command = TtlCommand(repository)
    }

    @Test
    fun `TTL이 설정된 키의 남은 시간을 초 단위로 반환한다`() {
        repository.set("key", "value".toByteArray())
        repository.expire("key", 10)
        val args = listOf(
            RESPValue.BulkString("TTL".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        clock.advanceBy(3000)

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(7)
    }

    @Test
    fun `TTL이 설정되지 않은 키는 -1을 반환한다`() {
        repository.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("TTL".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(-1)
    }

    @Test
    fun `존재하지 않는 키는 -2를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("TTL".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(-2)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("TTL".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
