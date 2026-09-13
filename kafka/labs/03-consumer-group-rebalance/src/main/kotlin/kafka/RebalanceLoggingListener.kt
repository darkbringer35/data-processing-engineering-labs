package lab.kafka

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import java.time.Instant

class RebalanceLoggingListener(
    private val consumerId: String
) : ConsumerRebalanceListener {
    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        log("partitions_revoked", partitions)
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        log("partitions_assigned", partitions)
    }

    private fun log(event: String, partitions: Collection<TopicPartition>) {
        println(
            "timestamp=${Instant.now()} " +
                "consumerId=$consumerId " +
                "event=$event " +
                "partitions=${partitions.joinToString()}"
        )
    }
}
