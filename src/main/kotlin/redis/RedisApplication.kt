package redis

import redis.server.RedisServer

fun main(args: Array<String>) {
    val server = RedisServer()
    Runtime.getRuntime().addShutdownHook(Thread {
        server.shutdown()
    })
    server.start()
}
