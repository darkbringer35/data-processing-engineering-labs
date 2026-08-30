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

    val events = listOf(
        OrderEvent(
            UUID.randomUUID().toString(),
            "order-1",
            EventType.ORDER_CREATED,
            Instant.now()
        ),
        OrderEvent(
            UUID.randomUUID().toString(),
            "noise-1",
            EventType.ORDER_CREATED,
            Instant.now()
        ),
        OrderEvent(
            UUID.randomUUID().toString(),
            "order-1",
            EventType.PAYMENT_REQUESTED,
            Instant.now()
        ),
        OrderEvent(
            UUID.randomUUID().toString(),
            "noise-2",
            EventType.PAYMENT_REQUESTED,
            Instant.now()
        ),
        OrderEvent(
            UUID.randomUUID().toString(),
            "order-1",
            EventType.PAYMENT_COMPLETED,
            Instant.now()
        ),
        OrderEvent(
            UUID.randomUUID().toString(),
            "noise-3",
            EventType.ORDER_COMPLETED,
            Instant.now()
        ),
        OrderEvent(
            UUID.randomUUID().toString(),
            "order-1",
            EventType.ORDER_COMPLETED,
            Instant.now()
        ),
    )

    KafkaProducer<String, String>(props).use { producer ->
        events.forEach { event ->
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
                    "eventType=${event.eventType} " +
                    "partition=${metadata.partition()} " +
                    "offset=${metadata.offset()}"
            )
        }
    }
}