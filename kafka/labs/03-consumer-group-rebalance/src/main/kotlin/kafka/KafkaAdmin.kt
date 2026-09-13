package lab.kafka

import lab.kafka.config.KafkaConfigLoader
import lab.kafka.config.TopicConfig
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic

private const val CREATE_TOPIC = "create-topic"
private const val DESCRIBE_TOPIC = "describe-topic"
private const val DESCRIBE_GROUP = "describe-group"

fun main(args: Array<String>) {
    val config = KafkaConfigLoader.load().kafka
    val command = args.singleOrNull()

    if (command == null) {
        printUsage()
        return
    }

    Admin.create(mapOf("bootstrap.servers" to config.bootstrapServers)).use { admin ->
        when (command) {
            CREATE_TOPIC -> createTopic(admin, config.topic)
            DESCRIBE_TOPIC -> describeTopic(admin, config.topic.name)
            DESCRIBE_GROUP -> describeConsumerGroup(admin, config.consumer.groupId)
            else -> error("Unknown admin command: $command")
        }
    }
}

private fun createTopic(admin: Admin, topic: TopicConfig) {
    val newTopic = topic.let({ NewTopic(it.name, it.partitions, it.replicationFactor)})
    admin.createTopics( listOf(newTopic))
        .all()
        .get()
}

private fun describeTopic(admin: Admin, topicName: String) {
    admin.describeTopics(listOf(topicName))
        .allTopicNames()
        .get()
        .let { println(it) }
}

private fun describeConsumerGroup(admin: Admin, groupId: String) {
    val description = admin.describeConsumerGroups(listOf(groupId))
        .all()
        .get()
        .getValue(groupId)

    val coordinator = description.coordinator()

    println("consumer-group:")
    println("  groupId: ${description.groupId()}")
    println("  state: ${description.state()}")
    println("  assignor: ${description.partitionAssignor()}")
    println("  coordinator: ${coordinator.id()}@${coordinator.host()}:${coordinator.port()}")
    println("  memberCount: ${description.members().size}")
    println("  members:")

    description.members()
        .sortedBy { it.clientId() }
        .forEach { member ->
            val partitions = member.assignment()
                .topicPartitions()
                .sortedWith(compareBy({ it.topic() }, { it.partition() }))
                .joinToString(prefix = "[", postfix = "]") {
                    "${it.topic()}-${it.partition()}"
                }

            println("    - clientId: ${member.clientId()}")
            println("      memberId: ${member.consumerId()}")
            println("      host: ${member.host()}")
            println("      partitions: $partitions")
        }
}

private fun printUsage() {
    println("Usage: runAdmin --args=<$CREATE_TOPIC|$DESCRIBE_TOPIC|$DESCRIBE_GROUP>")
}
