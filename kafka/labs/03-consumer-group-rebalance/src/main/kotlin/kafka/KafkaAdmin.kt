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
    admin.describeConsumerGroups(listOf(groupId))
        .all()
        .get()
        .let { println(it) }
}

private fun printUsage() {
    println("Usage: runAdmin --args=<$CREATE_TOPIC|$DESCRIBE_TOPIC|$DESCRIBE_GROUP>")
}
