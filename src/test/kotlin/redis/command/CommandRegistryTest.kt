package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.storage.OperationsBundle

class CommandRegistryTest {
    private lateinit var registry: CommandRegistry

    @BeforeEach
    fun setUp() {
        registry = CommandRegistry(OperationsBundle.create())
    }

    @Test
    fun `등록된 명령어를 찾을 수 있다`() {
        val command = registry.find("GET")

        assertThat(command).isNotNull
        assertThat(command?.name).isEqualTo("GET")
    }

    @Test
    fun `대소문자를 구분하여 명령어를 찾는다`() {
        val lowerCase = registry.find("get")
        val upperCase = registry.find("GET")

        assertThat(lowerCase).isNull()
        assertThat(upperCase).isNotNull
    }

    @Test
    fun `등록되지 않은 명령어는 null을 반환한다`() {
        val command = registry.find("NON_EXISTENT")

        assertThat(command).isNull()
    }

    @Test
    fun `모든 기본 명령어가 등록되어 있다`() {
        val expectedCommands = listOf(
            "GET", "SET", "SETNX",
            "INCR", "DECR", "INCRBY", "DECRBY",
            "MGET", "MSET",
            "DEL", "EXISTS", "EXPIRE", "PEXPIRE", "TTL", "PTTL", "PERSIST",
            "KEYS", "SCAN",
            "DBSIZE", "FLUSHDB",
            "PING", "ECHO", "COMMAND"
        )

        for (commandName in expectedCommands) {
            assertThat(registry.find(commandName))
                .`as`("Command $commandName should be registered")
                .isNotNull
        }
    }
}
