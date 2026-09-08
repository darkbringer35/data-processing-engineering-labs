package lab.kafka

import java.time.Instant

data class OrderEvent(
    val eventId: String,
    val orderId: String,
    val eventType: EventType,
    val timestamp: Instant
)

enum class EventType {
    ORDER_CREATED,
    PAYMENT_REQUESTED,
    PAYMENT_COMPLETED,
    ORDER_COMPLETED,
}