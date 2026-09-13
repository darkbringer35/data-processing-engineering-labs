package lab.kafka

import lab.kafka.config.KafkaConfigLoader
import lab.kafka.config.TopicConfig
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic

private const val CREATE_TOPIC = "create-topic"
private const val DESCRIBE_TOPIC = "describe-topic"
private const val DESCRIBE_GROUP = "describe-group"

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
        return
    }

    val config = KafkaConfigLoader.load().kafka
    val command = args.first()
    val options = args.drop(1)

    Admin.create(mapOf("bootstrap.servers" to config.bootstrapServers)).use { admin ->
        when (command) {
            CREATE_TOPIC -> {
                val partitions = parsePartitionCount(options, config.topic.partitions)
                createTopic(admin, config.topic, partitions)
            }
            DESCRIBE_TOPIC -> {
                requireNoOptions(command, options)
                describeTopic(admin, config.topic.name)
            }
            DESCRIBE_GROUP -> {
                requireNoOptions(command, options)
                describeConsumerGroup(admin, config.consumer.groupId)
            }
            else -> error("Unknown admin command: $command\n${usage()}")
        }
    }
}

private fun createTopic(admin: Admin, topic: TopicConfig, partitions: Int) {
    val newTopic = NewTopic(topic.name, partitions, topic.replicationFactor)
    admin.createTopics(listOf(newTopic))
        .all()
        .get()
}

private fun parsePartitionCount(options: List<String>, defaultPartitions: Int): Int {
    require(options.size <= 1) {
        "'$CREATE_TOPIC' accepts at most one partition count.\n${usage()}"
    }

    if (options.isEmpty()) {
        return defaultPartitions
    }

    val input = options.single()
    val partitions = input.toIntOrNull()

    requireNotNull(partitions) { "Partition count must be an integer: $input" }
    require(partitions > 0) { "Partition count must be greater than 0." }

    return partitions
}

private fun requireNoOptions(command: String, options: List<String>) {
    require(options.isEmpty()) { "'$command' does not accept arguments.\n${usage()}" }
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
    println(usage())
}

private fun usage() = "Usage: runAdmin --args=<$CREATE_TOPIC [partitions]|$DESCRIBE_TOPIC|$DESCRIBE_GROUP>"
