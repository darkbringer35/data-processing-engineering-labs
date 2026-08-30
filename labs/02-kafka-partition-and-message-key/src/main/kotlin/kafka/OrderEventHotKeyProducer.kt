package lab.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant
import java.util.Properties
import java.util.UUID

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
        repeat(100) { index ->
            val orderId = if (index < 80) {
                "hot-order"
            } else {
                "order-${index % 10}"
            }

            val event = OrderEvent(
                eventId = UUID.randomUUID().toString(),
                orderId = orderId,
                eventType = EventType.entries.random(),
                timestamp = Instant.now()
            )

            val json = mapper.writeValueAsString(event)

            val metadata = producer.send(
                ProducerRecord(
                    "order-events",
                    event.orderId,
                    json
                )
            ).get()

            println(
                "orderId=${event.orderId} " +
                    "partition=${metadata.partition()} " +
                    "offset=${metadata.offset()}"
            )
        }
    }
}