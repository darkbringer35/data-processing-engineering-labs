# Lab 02. Kafka Partition & Message Key

## Goals
- Kafka Topic을 여러 Partition으로 구성하고 메시지가 Partition에 분배되는 방식을 확인한다.
- Message Key 유무에 따라 메시지가 어떤 Partition으로 전달되는지 비교한다.
- 동일한 Key를 가진 메시지가 동일한 Partition으로 전달되는지 확인한다.
- Kafka의 메시지 순서 보장이 Topic 전체가 아닌 Partition 단위로 이루어진다는 것을 실험으로 확인한다.
- Partition Key 설계가 순서 보장과 병렬성에 어떤 영향을 주는지 이해한다.

## Implementation
- `order-events` Topic은 3개의 Partition으로 구성한다.
- Producer는 주문 이벤트를 생성해 Kafka에 전송한다.
- Consumer는 `order-events` Topic을 구독하고 수신한 Record의 메타데이터를 출력한다.
- 주문 이벤트는 `orderId`를 포함하며, 이후 실험에서 이 값을 Message Key로 사용한다.
- Producer와 Consumer 출력에서 Partition과 Offset을 확인할 수 있도록 구성한다.
- 실험 단계에 따라 Producer의 Message Key 사용 여부와 이벤트 생성 방식을 변경하며 Partition 분배와 순서 보장 동작을 비교한다.

## How to Run

### 1. 카프카 실행

```bash
docker compose up -d

# 상태 확인
docker ps
```

### 2. 토픽 생성
```bash
# 토픽의 파티션 갯수 조정
docker exec lab02-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1
```

### 3. 프로듀서 실행
```bash
./gradlew runProducer
```

### 4. 컨슈머 실행
```bash
./gradlew runConsumer
```

## Experiment 1. Produce Records Without Key
### 목적
- Message Key를 지정하지 않은 상태에서 주문 이벤트를 전송하고, Kafka Producer가 Record를 여러 Partition에 어떻게 배치하는지 확인한다.
- 동일한 orderId를 가진 이벤트가 같은 Partition에 배치되는지, 각 Partition의 Offset이 독립적으로 증가하는지 확인한다.

### 명령어
```bash
./gradlew runConsumer
./gradlew runProducer
```

Producer는 Message Key 없이 100개의 주문 이벤트를 전송한다.
```kotlin
ProducerRecord(
    "order-events",
    json
)
```
### 실제 출력

#### 1차 실행 — 100 Records

Producer 출력 일부

![Producer records without message key](images/producer-records-without-key.png)

Consumer 출력 일부

![Consumer records without message key](images/consumer-records-without-key.png)

#### 2차 실행 — 1000 Records
- 이벤트 수를 1000개로 늘려 다시 실행한 결과 Partition 2에도 Record가 배치되는 것을 확인한다.
- 따라서 첫 번째 실행에서 Partition 2가 사용되지 않은 것은 Partition이 동작하지 않는 문제가 아니라, 해당 실행에서 나타난 Partition 선택 결과였다.

### 결과
- Message Key를 전달하지 않았기 때문에 Consumer에서 모든 Record의 key가 null로 확인되었다.
- 각 Partition의 Offset은 독립적으로 관리되었다.
- 동일한 orderId를 가진 이벤트가 서로 다른 Partition에 배치되는 것을 확인했다.
- 3개의 Partition을 생성했지만 이번 실행에서는 Partition 0과 1만 사용되었고, Partition 2에는 Record가 배치되지 않았다.
- 이벤트 수를 1000개로 늘리자 Partition 0,1,2가 전부 사용되는 것을 확인했다.

### 해석 및 궁금증
- Message Key를 지정하지 않으면 orderId는 Value 내부의 데이터일 뿐이므로 Partition 선택에 영향을 주지 않는다.
    - 동일한 orderId를 가진 이벤트가 서로 다른 Partition에 배치되었으므로, 현재 구조에서는 동일 주문의 이벤트가 하나의 Partition에 배치된다는 보장이 없다.
- 100개의 Record를 전송했을 때는 Partition 2가 전혀 사용되지 않았지만, 1000개의 Record를 전송했을 때는 Partition 2도 사용되었다.
    - 이를 통해 Key가 없는 Record가 단순한 RR 방식으로 모든 Partition에 균등하게 배치되는 것은 아니며, 짧은 실행 결과만으로 Partition 사용 분포를 일반화해서는 안 된다는 점을 확인했다.
- Kafka Producer는 Key가 없는 Record를 전송할 때 batching 효율을 높이기 위해 sticky 방식으로 특정 Partition을 일정 기간 사용하는 것으로 보인다.
  - 실제로 Record가 Partition 사이에 고르게 섞여 배치되지 않고, 한 Partition에 연속적으로 생성된 뒤 다른 Partition으로 이동하는 패턴이 관찰되었다.
- 다음 실험에서는 orderId를 Message Key로 지정하고, 동일한 Key를 가진 Record가 항상 동일한 Partition으로 전달되는지 확인한다.


## Troubleshooting

## Key Takeaways
