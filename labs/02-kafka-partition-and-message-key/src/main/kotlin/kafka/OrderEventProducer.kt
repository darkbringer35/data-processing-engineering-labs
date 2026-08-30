package lab.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant
import java.util.Properties
import java.util.UUID
import kotlin.random.Random

fun main() {
    val config = KafkaConfigLoader.load().kafka

    val props = Properties().apply {
        put("bootstrap.servers", config.bootstrapServers)
        put("key.serializer", config.producer.keySerializer)
        put("value.serializer", config.producer.valueSerializer)
    }

    val mapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())

    KafkaProducer<String, String>(props).use { producer ->
        repeat(1000) {
            val orderId = "order-${Random.nextInt(1, 11)}"

            val event = OrderEvent(
                eventId = UUID.randomUUID().toString(),
                orderId = orderId,
                eventType = EventType.entries.random(),
                timestamp = Instant.now()
            )

            val json = mapper.writeValueAsString(event)

            // 첫 번째 실험에서는 message key를 지정하지 않고 이벤트를 전송한다.
            // key가 없는 메시지가 여러 partition에 어떻게 분배되는지 확인한다.
            val metadata = producer.send(
                ProducerRecord(
                    "order-events",
                    json
                )
            ).get()

            println(
                "orderId=${event.orderId} " +
                "eventType=${event.eventType} " +
                "partition=${metadata.partition()} " +
                "offset=${metadata.offset()}"
            )
        }
    }
}