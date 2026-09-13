package lab.kafka

import lab.kafka.config.KafkaConfigLoader
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.Instant
import java.util.Properties

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

    KafkaConsumer<String, String>(props).use { consumer ->
        consumer.subscribe(
            listOf(config.topic.name),
            RebalanceLoggingListener(consumerId)
        )

        while (true) {
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
    }
}
