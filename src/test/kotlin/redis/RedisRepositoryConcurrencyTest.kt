package redis

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class RedisRepositoryConcurrencyTest {

    @RepeatedTest(10)
    fun `여러 스레드에서 동시에 set과 get을 수행해도 데이터 일관성이 유지된다`() {
        val repository = RedisRepository()
        val threadCount = 100
        val barrier = CyclicBarrier(threadCount)
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(threadCount) { index ->
            executor.submit {
                barrier.await()
                val key = "key-$index"
                val value = "value-$index".toByteArray()

                repository.set(key, value)
                val result = repository.get(key)

                assertThat(result).isNotNull
                assertThat(String(result!!)).isEqualTo("value-$index")
                latch.countDown()
            }
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue()
        executor.shutdown()
    }

    @RepeatedTest(10)
    fun `여러 스레드에서 같은 키에 동시에 set해도 마지막 값이 유지된다`() {
        val repository = RedisRepository()
        val threadCount = 100
        val barrier = CyclicBarrier(threadCount)
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val key = "shared-key"

        repeat(threadCount) { index ->
            executor.submit {
                barrier.await()
                repository.set(key, "value-$index".toByteArray())
                latch.countDown()
            }
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue()

        val result = repository.get(key)
        assertThat(result).isNotNull
        assertThat(String(result!!)).startsWith("value-")

        executor.shutdown()
    }

    @RepeatedTest(10)
    fun `TTL이 설정된 키에 여러 스레드가 동시에 접근해도 예외가 발생하지 않는다`() {
        val repository = RedisRepository()
        val threadCount = 100
        val barrier = CyclicBarrier(threadCount)
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val exceptionCount = AtomicInteger(0)

        repeat(threadCount) { index ->
            val key = "key-${index % 10}"
            repository.set(key, "value-$index".toByteArray())
            repository.expire(key, 1)
        }

        repeat(threadCount) { index ->
            executor.submit {
                try {
                    barrier.await()
                    val key = "key-${index % 10}"

                    repeat(100) {
                        repository.get(key)
                        repository.set(key, "new-value-$index".toByteArray())
                        repository.expire(key, 1)
                    }
                } catch (e: Exception) {
                    exceptionCount.incrementAndGet()
                } finally {
                    latch.countDown()
                }
            }
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue()
        assertThat(exceptionCount.get()).isEqualTo(0)
        executor.shutdown()
    }

    @RepeatedTest(10)
    fun `TTL 만료 시점에 여러 스레드가 동시에 get을 호출해도 예외가 발생하지 않는다`() {
        val repository = RedisRepository()
        val threadCount = 50
        val barrier = CyclicBarrier(threadCount)
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val exceptionCount = AtomicInteger(0)

        repeat(100) { index ->
            val key = "expiring-key-$index"
            repository.set(key, "value".toByteArray())
            repository.expire(key, 1)
        }

        Thread.sleep(900)

        repeat(threadCount) {
            executor.submit {
                try {
                    barrier.await()
                    repeat(200) { index ->
                        val key = "expiring-key-${index % 100}"
                        repository.get(key)
                    }
                } catch (e: Exception) {
                    exceptionCount.incrementAndGet()
                } finally {
                    latch.countDown()
                }
            }
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue()
        assertThat(exceptionCount.get()).isEqualTo(0)
        executor.shutdown()
    }

    @RepeatedTest(10)
    fun `백그라운드 TTL 정리와 set, get, expire가 동시에 수행되어도 예외가 발생하지 않는다`() {
        val repository = RedisRepository()
        val threadCount = 100
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val exceptionCount = AtomicInteger(0)
        val operationCount = AtomicInteger(0)

        repeat(threadCount) { threadIndex ->
            executor.submit {
                try {
                    repeat(500) { opIndex ->
                        val key = "key-${(threadIndex * 500 + opIndex) % 50}"

                        when (opIndex % 4) {
                            0 -> repository.set(key, "value-$opIndex".toByteArray())
                            1 -> repository.get(key)
                            2 -> repository.expire(key, 1)
                            3 -> repository.delete(key)
                        }
                        operationCount.incrementAndGet()
                    }
                } catch (e: Exception) {
                    exceptionCount.incrementAndGet()
                } finally {
                    latch.countDown()
                }
            }
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue()
        assertThat(exceptionCount.get()).isEqualTo(0)
        assertThat(operationCount.get()).isEqualTo(threadCount * 500)
        executor.shutdown()
    }

    @Test
    fun `만료된 키는 get 호출 시 null을 반환하고 삭제된다`() {
        val repository = RedisRepository()
        val key = "test-key"

        repository.set(key, "value".toByteArray())
        repository.expire(key, 1)

        assertThat(repository.get(key)).isNotNull

        Thread.sleep(1100)

        assertThat(repository.get(key)).isNull()
        assertThat(repository.get(key)).isNull()
    }

    @RepeatedTest(10)
    fun `동시에 같은 키를 삭제하고 설정해도 데이터 무결성이 유지된다`() {
        val repository = RedisRepository()
        val threadCount = 50
        val barrier = CyclicBarrier(threadCount)
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val key = "contested-key"

        repository.set(key, "initial".toByteArray())

        repeat(threadCount) { index ->
            executor.submit {
                barrier.await()
                if (index % 2 == 0) {
                    repository.delete(key)
                } else {
                    repository.set(key, "value-$index".toByteArray())
                }
                latch.countDown()
            }
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue()

        val result = repository.get(key)
        if (result != null) {
            assertThat(String(result)).startsWith("value-")
        }

        executor.shutdown()
    }
}
