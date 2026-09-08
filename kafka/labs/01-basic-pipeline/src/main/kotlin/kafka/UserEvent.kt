package lab.kafka

import java.time.Instant

data class UserEvent(
    val eventId: String,
    val userId: String,
    val eventType: EventType,
    val timestamp: Instant
)

enum class EventType {
    VIEW,
    CLICK,
    LIKE,
    PURCHASE
}