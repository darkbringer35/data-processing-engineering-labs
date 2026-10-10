package lab.kafka

import lab.kafka.config.KafkaConfigLoader
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

fun main(args: Array<String>) {
    val consumerId = args.singleOrNull() ?: error("Consumer ID is required.")

    val config = KafkaConfigLoader.load().kafka

    val props = Properties().apply {
        put("client.id", consumerId)
        put("bootstrap.servers", config.bootstrapServers)
        put("group.id", config.consumer.groupId)
        put("key.deserializer", config.consumer.keyDeserializer)
        put("value.deserializer", config.consumer.valueDeserializer)
        put("auto.offset.reset", config.consumer.autoOffsetReset)
    }

    val consumer = KafkaConsumer<String, String>(props)
    val shutdownRequested = AtomicBoolean(false)
    val consumerThread = Thread.currentThread()
    val pid = ProcessHandle.current().pid()

    val shutdownHook = Thread({
        if (shutdownRequested.compareAndSet(false, true)) {
            logLifecycle(consumerId, "shutdown_requested", "pid=$pid")
        }

        consumer.wakeup()
        consumerThread.join()
    }, "consumer-shutdown-$consumerId")

    Runtime.getRuntime().addShutdownHook(shutdownHook)
    logLifecycle(consumerId, "consumer_started", "pid=$pid")

    try {
        consumer.subscribe(
            listOf(config.topic.name),
            RebalanceLoggingListener(consumerId)
        )

        while (!shutdownRequested.get()) {
            val records = consumer.poll(Duration.ofSeconds(1))

            for (record in records) {
                val processedDate = Instant.now()

                println(
                    "processedDate=$processedDate " +
                        "consumerId=$consumerId " +
                        "partition=${record.partition()} " +
                        "offset=${record.offset()} " +
                        "key=${record.key()} " +
                        "value=${record.value()}"
                )
            }
        }
    } catch (exception: WakeupException) {
        if (!shutdownRequested.get()) {
            throw exception
        }
    } finally {
        logLifecycle(consumerId, "consumer_close_started")
        consumer.close()
        logLifecycle(consumerId, "consumer_close_completed")
    }
}

private fun logLifecycle(consumerId: String, event: String, detail: String? = null) {
    println(
        buildString {
            append("timestamp=${Instant.now()} consumerId=$consumerId event=$event")
            if (detail != null) {
                append(" $detail")
            }
        }
    )
}
