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

    // 현재는 StringDeserializer를 사용하므로 value는 JSON 문자열 그대로 반환된다.
    // 실제 애플리케이션에서는 별도의 역직렬화를 거쳐 UserEvent로 변환할 수 있다.
    KafkaConsumer<String, String>(props).use { consumer ->
        consumer.subscribe(listOf("user-events"))

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