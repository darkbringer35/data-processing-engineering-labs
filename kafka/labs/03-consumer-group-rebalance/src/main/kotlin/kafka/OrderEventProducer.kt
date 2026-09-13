package lab.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import lab.kafka.config.KafkaConfigLoader
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant
import java.util.Properties
import java.util.UUID
import kotlin.random.Random

private const val CONTINUOUS_MODE = "continuous"
private const val BATCH_RECORD_COUNT = 100

fun main(args: Array<String>) {
    val continuous = when (args.toList()) {
        emptyList<String>() -> false
        listOf(CONTINUOUS_MODE) -> true
        else -> error("Usage: runProducer [--args=\"$CONTINUOUS_MODE\"]")
    }

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
        if (continuous) {
            publishContinuously(producer, mapper, config.topic.name)
        } else {
            repeat(BATCH_RECORD_COUNT) {
                publishOrderEvent(producer, mapper, config.topic.name)
            }
        }
    }
}

private fun publishContinuously(
    producer: KafkaProducer<String, String>,
    mapper: ObjectMapper,
    topicName: String
) {
    while (true) {
        publishOrderEvent(producer, mapper, topicName)
        Thread.sleep(100)
    }
}

private fun publishOrderEvent(
    producer: KafkaProducer<String, String>,
    mapper: ObjectMapper,
    topicName: String
) {
    val orderId = "order-${Random.nextInt(1, 11)}"
    val event = OrderEvent(
        eventId = UUID.randomUUID().toString(),
        orderId = orderId,
        eventType = EventType.entries.random(),
        timestamp = Instant.now()
    )

    val metadata = producer.send(
        ProducerRecord(
            topicName,
            event.orderId,
            mapper.writeValueAsString(event)
        )
    ).get()

    println(
        "orderId=${event.orderId} " +
            "eventType=${event.eventType} " +
            "partition=${metadata.partition()} " +
            "offset=${metadata.offset()}"
    )
}
