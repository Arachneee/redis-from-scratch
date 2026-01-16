package redis.error

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue

class RedisErrorsTest {

    @Test
    fun `unknownCommand는 올바른 에러 메시지를 반환한다`() {
        val result = RedisErrors.unknownCommand("INVALID")

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
        assertThat(result.message).isEqualTo("ERR unknown command 'INVALID'")
    }

    @Test
    fun `unknownCommand는 null 명령어도 처리한다`() {
        val result = RedisErrors.unknownCommand(null)

        assertThat(result.message).isEqualTo("ERR unknown command 'null'")
    }

    @Test
    fun `unknownSubcommand는 올바른 에러 메시지를 반환한다`() {
        val result = RedisErrors.unknownSubcommand("INVALID")

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
        assertThat(result.message).isEqualTo("ERR unknown subcommand 'INVALID'")
    }

    @Test
    fun `wrongNumberOfArguments는 올바른 에러 메시지를 반환한다`() {
        val result = RedisErrors.wrongNumberOfArguments("get")

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
        assertThat(result.message).isEqualTo("ERR wrong number of arguments for 'get' command")
    }

    @Test
    fun `syntaxError는 올바른 에러 메시지를 반환한다`() {
        val result = RedisErrors.syntaxError()

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
        assertThat(result.message).isEqualTo("ERR syntax error")
    }

    @Test
    fun `invalidInteger는 올바른 에러 메시지를 반환한다`() {
        val result = RedisErrors.invalidInteger()

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
        assertThat(result.message).isEqualTo("ERR value is not an integer or out of range")
    }

    @Test
    fun `wrongType은 올바른 에러 메시지를 반환한다`() {
        val result = RedisErrors.wrongType()

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
        assertThat(result.message).isEqualTo("WRONGTYPE Operation against a key holding the wrong kind of value")
    }
}
