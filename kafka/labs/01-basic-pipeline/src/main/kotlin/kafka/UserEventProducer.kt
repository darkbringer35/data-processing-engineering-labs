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

    // UserEvent의 Instant를 JSON으로 직렬화하기 위해 JavaTimeModule을 등록한다.
    val mapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())

    KafkaProducer<String, String>(props).use { producer ->
        repeat(100) {
            val userId = "user-${Random.nextInt(1, 11)}"

            val event = UserEvent(
                eventId = UUID.randomUUID().toString(),
                userId = userId,
                eventType = EventType.entries.random(),
                timestamp = Instant.now()
            )

            val json = mapper.writeValueAsString(event)

            // 같은 userId를 key로 사용하면 partition 수가 늘어나도
            // 동일 사용자의 이벤트는 같은 partition에 배치되어 순서를 유지한다.
            val metadata = producer.send(
                ProducerRecord(
                    "user-events",
                    userId,
                    json
                )
                // send()는 비동기지만, 실습에서는 get으로 broker의 응답을 기다려 record에 할당된 partition과 offset을 즉시 확인한다.
                // 실제 대량 처리에서는 매번 get()하면 처리량이 감소할 수 있다.
            ).get()

            println(
                "event=$event partition=${metadata.partition()} offset=${metadata.offset()}"
            )
        }
    }
}