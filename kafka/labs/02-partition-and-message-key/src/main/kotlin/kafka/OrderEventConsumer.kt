package lab.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.Properties

fun main() {
    val config = KafkaConfigLoader.load().kafka

    val props = Properties().apply {
        put("bootstrap.servers", config.bootstrapServers)
        put("group.id", config.consumer.groupId)
        put("key.deserializer", config.consumer.keyDeserializer)
        put("value.deserializer", config.consumer.valueDeserializer)
        put("auto.offset.reset", config.consumer.autoOffsetReset)
    }

    KafkaConsumer<String, String>(props).use { consumer ->
        consumer.subscribe(listOf("order-events"))

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))

            for (record in records) {
                println(
                    "partition=${record.partition()} " +
                        "offset=${record.offset()} " +
                        "key=${record.key()} " +
                        "value=${record.value()}"
                )
            }
        }
    }
}