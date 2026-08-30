package lab.kafka.config

data class ApplicationConfig(
    val kafka: KafkaConfig
)

data class KafkaConfig(
    val bootstrapServers: String,
    val producer: ProducerConfig,
    val consumer: ConsumerConfig
)

data class ProducerConfig(
    val keySerializer: String,
    val valueSerializer: String
)

data class ConsumerConfig(
    val groupId: String,
    val keyDeserializer: String,
    val valueDeserializer: String,
    val autoOffsetReset: String
)