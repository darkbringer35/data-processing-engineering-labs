package lab.kafka

import lab.kafka.config.KafkaConfigLoader
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.clients.consumer.RoundRobinAssignor
import org.apache.kafka.clients.consumer.StickyAssignor
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

private const val COMMIT_DELAY_ENV = "LAB_COMMIT_DELAY_MS"

fun main(args: Array<String>) {
    require(args.size in 1..6) {
        "Usage: <consumer-id> [bootstrap-servers] [processing-delay-ms] [group-id] [assignor] [group-instance-id]"
    }

    val config = KafkaConfigLoader.load().kafka

    val consumerId = args[0]
    val bootstrapServers = args.getOrNull(1) ?: config.bootstrapServers
    val processingDelayMs = args.getOrNull(2)?.toLongOrNull() ?: 0L
    val groupId = args.getOrNull(3) ?: config.consumer.groupId
    val assignor = args.getOrNull(4)
    val assignmentStrategyClassName = resolveAssignmentStrategy(assignor)
    val groupInstanceId = args.getOrNull(5)
    val commitDelayMs = System.getenv(COMMIT_DELAY_ENV)?.let { input ->
        requireNotNull(input.toLongOrNull()) {
            "$COMMIT_DELAY_ENV must be an integer: $input"
        }
    } ?: 0L

    require(groupId.isNotBlank()) {
        "Group ID must not be blank."
    }

    require(groupInstanceId == null || groupInstanceId.isNotBlank()) {
        "Group instance ID must not be blank."
    }

    require(processingDelayMs >= 0) {
        "Processing delay must be zero or greater."
    }

    require(commitDelayMs >= 0) {
        "$COMMIT_DELAY_ENV must be zero or greater."
    }

    val props = Properties().apply {
        put("client.id", consumerId)
        put("bootstrap.servers", bootstrapServers)
        put("group.id", groupId)
        put("key.deserializer", config.consumer.keyDeserializer)
        put("value.deserializer", config.consumer.valueDeserializer)
        put("auto.offset.reset", config.consumer.autoOffsetReset)
        put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, config.consumer.heartbeatIntervalMs)
        put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.consumer.sessionTimeoutMs)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.consumer.maxPollRecords)
        put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, config.consumer.maxPollIntervalMs)
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        assignmentStrategyClassName?.let {
            put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, it)
        }
        groupInstanceId?.let {
            put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, it)
        }
    }

    val consumer = KafkaConsumer<String, String>(props)
    val shutdownRequested = AtomicBoolean(false)
    val consumerThread = Thread.currentThread()
    val pid = ProcessHandle.current().pid()

    val shutdownHook = Thread({
        if (shutdownRequested.compareAndSet(false, true)) {
            logLifecycle(consumerId, "shutdown_requested", "pid=$pid")
        }

        consumerThread.join()
    }, "consumer-shutdown-$consumerId")

    Runtime.getRuntime().addShutdownHook(shutdownHook)
    logLifecycle(
        consumerId,
        "consumer_started",
        "pid=$pid bootstrapServers=$bootstrapServers " +
            "groupId=$groupId " +
            "assignor=${assignor ?: "default"} " +
            "groupInstanceId=${groupInstanceId ?: "dynamic"} " +
            "heartbeatIntervalMs=${config.consumer.heartbeatIntervalMs} " +
            "sessionTimeoutMs=${config.consumer.sessionTimeoutMs} " +
            "maxPollRecords=${config.consumer.maxPollRecords} " +
            "maxPollIntervalMs=${config.consumer.maxPollIntervalMs} " +
            "processingDelayMs=$processingDelayMs " +
            "commitDelayMs=$commitDelayMs"
    )

    try {
        consumer.subscribe(
            listOf(config.topic.name),
            RebalanceLoggingListener(consumerId)
        )

        while (!shutdownRequested.get()) {
            val records = consumer.poll(Duration.ofSeconds(1))

            for (record in records) {
                if (shutdownRequested.get()) {
                    logLifecycle(
                        consumerId,
                        "record_rejected_after_shutdown",
                        "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}"
                    )
                    break
                }

                val recordDetail =
                    "topic=${record.topic()} " +
                        "partition=${record.partition()} " +
                        "offset=${record.offset()} " +
                        "key=${record.key()}"

                logLifecycle(consumerId, "processing_started", recordDetail)

                Thread.sleep(processingDelayMs)

                val processedDate = Instant.now()

                println(
                    "processedDate=$processedDate " +
                        "processingDelayMs=$processingDelayMs " +
                        "consumerId=$consumerId " +
                        "partition=${record.partition()} " +
                        "offset=${record.offset()} " +
                        "key=${record.key()} " +
                        "value=${record.value()}"
                )

                logLifecycle(consumerId, "processing_completed", recordDetail)

                Thread.sleep(commitDelayMs)

                logLifecycle(
                    consumerId,
                    "offset_commit_started",
                    "$recordDetail nextOffset=${record.offset() + 1}"
                )

                consumer.commitSync(
                    mapOf(
                        TopicPartition(record.topic(), record.partition()) to
                            OffsetAndMetadata(record.offset() + 1)
                    )
                )

                logLifecycle(
                    consumerId,
                    "offset_commit_completed",
                    "$recordDetail nextOffset=${record.offset() + 1}"
                )
            }
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

private fun resolveAssignmentStrategy(assignor: String?): String? {
    return when (assignor) {
        null -> null
        "range" -> RangeAssignor::class.java.name
        "roundrobin" -> RoundRobinAssignor::class.java.name
        "sticky" -> StickyAssignor::class.java.name
        "cooperative-sticky" -> CooperativeStickyAssignor::class.java.name
        else -> error(
            "Unknown assignor: $assignor. " +
                "Supported assignors: range, roundrobin, sticky, cooperative-sticky"
        )
    }
}
