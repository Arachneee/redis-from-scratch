package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.storage.FakeClock
import redis.storage.RedisRepository

class CommandRegistryTest {
    private lateinit var registry: CommandRegistry

    @BeforeEach
    fun setUp() {
        val repository = RedisRepository(FakeClock())
        registry = CommandRegistry(repository)
    }

    @Test
    fun `등록된 명령어를 찾을 수 있다`() {
        assertThat(registry.find("GET")).isNotNull
        assertThat(registry.find("SET")).isNotNull
        assertThat(registry.find("DEL")).isNotNull
        assertThat(registry.find("PING")).isNotNull
    }

    @Test
    fun `대문자로 명령어를 찾을 수 있다`() {
        assertThat(registry.find("GET")).isNotNull
        assertThat(registry.find("GET")?.name).isEqualTo("GET")
    }

    @Test
    fun `등록되지 않은 명령어는 null을 반환한다`() {
        assertThat(registry.find("UNKNOWN")).isNull()
    }

    @Test
    fun `COMMAND 명령어가 등록되어 있다`() {
        assertThat(registry.find("COMMAND")).isNotNull
    }

    @Test
    fun `모든 기본 명령어가 등록되어 있다`() {
        val expectedCommands = listOf(
            "GET", "SET", "DEL", "EXPIRE", "TTL",
            "EXISTS", "DBSIZE", "FLUSHDB", "PERSIST",
            "PING", "ECHO", "COMMAND"
        )

        expectedCommands.forEach { commandName ->
            assertThat(registry.find(commandName))
                .withFailMessage("$commandName should be registered")
                .isNotNull
        }
    }
}
