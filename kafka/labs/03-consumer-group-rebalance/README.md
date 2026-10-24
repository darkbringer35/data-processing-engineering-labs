# Lab 03. Kafka Consumer Group & Rebalance

## Goals

- Consumer Group 안에서 Partition이 Consumer에게 할당되는 방식을 확인한다.
- Consumer가 추가되거나 종료될 때 Rebalance가 발생하는 과정을 관찰한다.
- Consumer 수와 Partition 수의 관계를 이해한다.
- Rebalance 과정에서 Partition이 revoke되고 다시 assign되는 시점을 확인한다.
- Kafka AdminClient로 Topic과 Consumer Group 상태를 조회하고, Kafka CLI 결과와 교차 검증한다.

## Implementation

이번 Lab은 하나의 Kafka Topic과 Producer, Consumer Group으로 구성한다.

- `order-events` Topic은 기본적으로 6개의 Partition으로 구성하며, 실험에 따라 생성 명령에서 Partition 수를 지정할 수 있다.
- `OrderEventProducer`
  - Lab 02와 동일하게 `orderId`를 Message Key로 사용해 주문 이벤트 100건을 전송한다.
- `OrderEventConsumer`
  - 모든 컨슈머는 기본적으로 `lab03-order-processors` 컨슈머 그룹에 참여한다.
  - 실행 인자로 받은 식별자를 `client.id`와 Record 로그에 사용한다.
  - 시작할 때 실제 컨슈머 JVM의 PID를 출력한다.
  - `RebalanceLoggingListener`로 Assigned/Revoked Partition을 기록한다.
  - 수신한 Record의 Partition, Offset, Key와 Value를 출력한다.
  - JVM 종료 요청을 받으면 shutdown hook에서 종료 상태를 기록하고 `consumer.wakeup()`으로 `poll()`을 중단한다.
  - `WakeupException`이 종료 요청으로 발생했는지 확인한 뒤 컨슈머 실행 스레드의 `finally`에서 `consumer.close()`를 호출하며, Close 시작과 완료 시각을 기록한다.
- `KafkaAdmin`
  - Producer나 Consumer와 분리된 실행 진입점으로 구성한다.
  - `create-topic [partitions]`, `describe-topic`, `describe-group` 명령을 제공한다.
  - `create-topic`의 Partition 수를 생략하면 `application.yml`의 값을 사용하고, 양의 정수를 전달하면 해당 값으로 Topic을 생성한다.
  - 각 명령에서 허용하지 않는 추가 인자는 실행 전에 거부한다.
  - AdminClient의 비동기 결과가 완료될 때까지 기다린 뒤 성공과 실패를 호출자에게 전달한다.
  - Producer 또는 Consumer 시작 과정에서 Topic을 암묵적으로 생성하지 않는다.
  - Kafka CLI는 AdminClient 결과를 독립적으로 검증하는 관찰 도구로 유지한다.

첫 번째 실험에서는 Lab 02의 실행 구조를 유지한 상태에서 별도의 AdminClient 진입점을 추가했다. `create-topic`, `describe-topic`, `describe-group` 명령으로 Topic을 준비하고 단일 Consumer의 Partition Assignment를 조회한다.

## How to Run

### 1. 카프카 실행

```bash
docker compose up -d
docker compose ps
```

### 2. 토픽 생성

JVM AdminClient로 Topic을 준비하고, Kafka CLI는 결과를 교차 검증하는 용도로 사용한다.

지원하는 Admin 명령을 확인한다.

```bash
./gradlew runAdmin
```

다음 명령을 순서대로 실행한다.

```bash
./gradlew runAdmin --args="create-topic"
./gradlew runAdmin --args="describe-topic"
```

`create-topic`에 Partition 수를 생략하면 `application.yml`에 설정한 6개를 사용한다. 다른 수가 필요한 실험에서는 양의 정수 하나를 추가한다.

```bash
./gradlew runAdmin --args="create-topic 3"
```

Topic의 Partition 수를 확인한다.

```bash
docker exec lab03-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic order-events
```

### 3. 빌드와 테스트

```bash
./gradlew clean test
```

### 4. 프로듀서 실행

별도 Terminal에서 실행한다.

```bash
./gradlew runProducer
```

### 5. 컨슈머 실행

다른 Terminal에서 실행한다.

```bash
./gradlew runConsumer --args="c1"
```

### 6. 컨슈머 그룹 확인

Experiment 1에서 AdminClient 기반 조회를 구현한 뒤 JVM 출력과 아래 Kafka CLI 출력을 비교한다.

```bash
./gradlew runAdmin --args="describe-group"
```

```bash
docker exec lab03-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group lab03-order-processors \
  --members \
  --verbose
```

## Experiment 1. Assign Six Partitions to One Consumer

### 목적

- 6개의 Partition을 가진 Topic을 하나의 Consumer가 구독했을 때 전체 Partition이 해당 Consumer에 할당되는지 확인한다.
- Consumer가 실제로 처리한 Partition과 Kafka CLI의 Assignment가 일치하는지 확인한다.
- Partition마다 Offset이 독립적으로 증가하는지 확인한다.
- Topic 생성·조회와 Consumer Group 조회를 Producer/Consumer가 아닌 별도 AdminClient 진입점에서 수행한다.
- AdminClient와 Kafka CLI가 같은 Topic 및 Assignment 상태를 보여주는지 교차 검증한다.

### 명령어

Kafka를 시작하고 AdminClient로 Topic을 생성·조회했다.

```bash
docker compose up -d
./gradlew runAdmin --args="create-topic"
./gradlew runAdmin --args="describe-topic"
```

Kafka CLI로 Topic 구성을 교차 검증했다.

```bash
docker exec lab03-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic order-events
```

서로 다른 Terminal에서 Consumer와 Producer를 실행했다.

```bash
# Ex2 변경 반영
./gradlew runConsumer --args="c1"
./gradlew runProducer
```

AdminClient와 Kafka CLI로 `lab03-order-processors` Group을 각각 조회했다.

```bash
./gradlew runAdmin --args="describe-group"

docker exec lab03-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group lab03-order-processors \
  --members \
  --verbose
```

### 실제 출력

Topic은 최초 생성에 성공했으며, 동일 이름으로 다시 생성했을 때 `TopicExistsException`이 반환되었다.

![Topic 최초 생성과 중복 생성 오류](images/order-events-topic-created-and-duplicate-rejected.png)

AdminClient와 Kafka CLI 모두 `order-events`의 Partition 수를 6개, Replication Factor를 1로 조회했다.

![AdminClient Topic 조회](images/order-events-topic-described-by-admin-client.png)

![Kafka CLI Topic 조회](images/order-events-topic-described-by-kafka-cli.png)

Producer가 `orderId`를 Key로 사용해 주문 이벤트 100건을 전송했다.

![Producer 주문 이벤트 전송](images/producer-order-events-published.png)

단일 Consumer가 Partition 0부터 5까지의 Record를 처리했다. 각 Partition의 Offset은 서로 독립적으로 증가했다.

![단일 Consumer Record 처리](images/consumer-records-single-member.png)

AdminClient 조회에서 Group은 `Stable`, Assignor는 `range`, Group Type은 `Classic`으로 나타났다. 단일 Member에는 Partition 0부터 5까지 할당되었다.

![AdminClient Consumer Group 조회](images/consumer-group-described-by-admin-client.png)

Kafka CLI에서도 동일한 Member 한 개와 Partition 6개의 Assignment `(0,1,2,3,4,5)`를 확인했다.

![Kafka CLI Consumer Group 조회](images/consumer-group-described-by-kafka-cli.png)

### 결과

| Consumer 수 | Group 상태 | Assignor | Assigned Partitions | 처리 Record 수 |
|---:|---|---|---|---:|
| 1 | Stable | range | 0, 1, 2, 3, 4, 5 | 100 |

| 검증 항목 | AdminClient | Kafka CLI | 비교 결과 |
|---|---|---|---|
| Topic Partition 수 | 6 | 6 | 일치 |
| Replication Factor | 1 | 1 | 일치 |
| Consumer 수 | 1 | 1 | 일치 |
| Assigned Partition 수 | 6 | 6 | 일치 |
| Assignment | 0, 1, 2, 3, 4, 5 | 0, 1, 2, 3, 4, 5 | 일치 |

단일 Consumer가 6개 Partition을 모두 할당받아 처리했다. Producer가 생성한 100건은 Partition별로 분산되었고, 단일 Consumer가 모든 Partition의 Record를 처리했다.

### 해석 및 궁금증

- 컨슈머 그룹의 병렬 처리 단위는 컨슈머가 아니라 파티션이다. 그룹에 멤버가 하나뿐이므로 구독 대상인 파티션 6개를 해당 컨슈머가 모두 할당받았다.
- 캡처에서 Partition별 Offset은 각각 증가하지만 서로 다른 Partition의 Record는 섞여 출력된다. 따라서 하나의 Consumer가 모든 Partition을 처리하더라도 Topic 전체 순서는 보장되지 않고 Partition 내부 순서만 관찰할 수 있다.
- AdminClient와 Kafka CLI의 결과는 이번 조회에서 일치했다.

## Experiment 2. Scale Consumers from One to Three

### 목적

- 같은 컨슈머 그룹의 컨슈머를 1개에서 2개, 3개로 늘릴 때 파티션이 어떻게 재할당되는지 확인한다.
- 컨슈머 증설 전후에 모든 컨슈머 로그에서 레코드 출력이 끊긴 간격을 관찰하고, 이 값이 리밸런싱 자체의 소요 시간과 다른 이유를 확인한다.
- 각 컨슈머를 구분하고 할당·반납된 파티션을 확인할 수 있도록 기본 컨슈머를 확장한다.
- 컨슈머를 늘리는 동안에도 레코드가 계속 들어오도록 프로듀서 실행 방식을 확장한다.

### 명령어

2번의 독립된 실험으로 나누어 실험했다.

첫 번째 실험에서는 c1에 파티션이 할당된 상태를 확인한 뒤 c2를 합류시키고, AdminClient로 멤버가 2개인 상태를 조회했다. 이 실험에서는 레코드가 계속 들어오지 않았으므로 처리 시간은 계산하지 않았다.

```bash
docker compose down
docker compose up -d
# Ex3 변경 반영
./gradlew runAdmin --args="create-topic 6"

./gradlew runConsumer --args="c1"
./gradlew runConsumer --args="c2"
./gradlew runAdmin --args="describe-group"
```

첫 번째 실험을 종료한 뒤 Kafka와 토픽을 다시 초기화했다. 두 번째 실험에서는 프로듀서를 `continuous` 모드로 실행해 100ms 간격으로 레코드를 보냈다. 이 상태에서 c1부터 다시 시작해 c2와 c3을 순서대로 합류시켰다.

```bash
docker compose down
docker compose up -d
# Ex3 변경 반영
./gradlew runAdmin --args="create-topic 6"

./gradlew runConsumer --args="c1"
./gradlew runProducer --args="continuous"
./gradlew runConsumer --args="c2"
./gradlew runConsumer --args="c3"
```

### 실제 출력

#### 첫 번째 실험: 파티션 할당 확인

첫 번째 실험에서 c1은 파티션 0~5를 모두 할당받았다.

![첫 번째 실행에서 c1이 단독으로 파티션을 할당받은 결과](images/consumer-c1-initial-assignment.png)

c2가 합류하자 c1은 기존 파티션 6개를 모두 반납한 뒤 파티션 0, 1, 2를 다시 할당받았다.

![첫 번째 실행에서 c2 합류 후 c1의 리밸런싱](images/consumer-c1-rebalance-after-c2-join.png)

c2는 파티션 3, 4, 5를 할당받았다.

![첫 번째 실행에서 c2가 할당받은 파티션](images/consumer-c2-assignment-after-join.png)

첫 번째 실험의 AdminClient 조회에서도 c1은 파티션 0~2, c2는 파티션 3~5를 소유한 것으로 확인되었다.

![첫 번째 실행에서 멤버가 2개인 컨슈머 그룹 조회](images/consumer-group-two-members-by-admin-client.png)

#### 두 번째 실험: 레코드가 유입되는 동안 컨슈머 증설

두 번째 실험은 Kafka와 토픽을 다시 초기화하고 c1부터 시작했다. 프로듀서가 `continuous` 모드로 실행되는 동안 c2가 합류하자 c1은 파티션 6개를 모두 반납한 뒤 파티션 0, 1, 2를 다시 할당받았다.

리밸런싱 직전 전체 로그의 마지막 레코드와 리밸런싱 이후 처음 출력된 레코드 사이에는 약 136ms의 간격이 있었다.

이 값은 리밸런싱 자체의 소요 시간이 아니다. 프로듀서의 100ms 전송 주기, 키에 따른 파티션 분포, 레코드를 가져오는 시간과 애플리케이션 출력 시간이 모두 포함되어 있다.

![c2 합류 시 c1의 리밸런싱과 전체 컨슈머의 레코드 출력 공백](images/consumer-c1-processing-gap-after-c2-join.png)

c2는 파티션 3, 4, 5를 할당받아 레코드 처리를 시작했다.

![멤버가 2개인 상태에서 c2의 레코드 처리](images/consumer-c2-processing-with-two-members.png)

c3가 합류하자 c1은 파티션 0~2를 반납한 뒤 파티션 0, 1을 할당받았다.

![c3 합류 시 c1의 리밸런싱](images/consumer-c1-rebalance-after-c3-join.png)

c2는 파티션 3~5를 반납한 뒤 파티션 2, 3을 할당받았다.

![c3 합류 시 c2의 리밸런싱](images/consumer-c2-rebalance-after-c3-join.png)

c3는 파티션 4, 5를 할당받고 레코드 처리를 시작했다. 멤버가 3개인 최종 상태는 세 컨슈머의 콜백 로그로 확인했으며 따로 브로커에 컨슈머 그룹 상태를 조회하지 않았다.

![c3의 최초 파티션 할당과 레코드 처리](images/consumer-c3-assignment-after-join.png)

### 결과

| 실행 | 컨슈머 수 | c1 파티션 | c2 파티션 | c3 파티션 | 소유자가 바뀐 파티션 수 | 전체 컨슈머의 레코드 출력 공백 |
|---|---:|---|---|---|---:|---:|
| 첫 번째: 할당 확인 | 1 | 0, 1, 2, 3, 4, 5 | - | - | 0 | 측정하지 않음 |
| 첫 번째: 할당 확인 | 2 | 0, 1, 2 | 3, 4, 5 | - | 3 | 측정하지 않음 |
| 두 번째: 지속 처리 | 1 | 0, 1, 2, 3, 4, 5 | - | - | 0 | - |
| 두 번째: 지속 처리 | 2 | 0, 1, 2 | 3, 4, 5 | - | 3 | 약 136ms |
| 두 번째: 지속 처리 | 3 | 0, 1 | 2, 3 | 4, 5 | 3 | 약 141ms |

전체 컨슈머의 레코드 출력 공백은 같은 `group.id`에 속한 모든 컨슈머의 로그를 하나의 시간축으로 합친 뒤, 리밸런싱 직전 마지막 `processedDate`와 리밸런싱 후 가장 빠른 `processedDate`의 차이로 계산했다. c2 합류 시에는 `12:04:59.202473Z`부터 `12:04:59.338110Z`까지 약 136ms, c3 합류 시에는 `12:05:17.248441Z`부터 `12:05:17.388997Z`까지 약 141ms다.

c2 합류 시에는 다음 시간축도 별도로 확인했다.

| 관찰 지점 | 시각 | 이전 지점과의 간격 |
|---|---|---:|
| c1의 리밸런싱 전 마지막 레코드 처리 | `12:04:59.202473Z` | - |
| c1의 `partitions_revoked` 콜백 | `12:04:59.228645Z` | 약 26ms |
| c1의 `partitions_assigned` 콜백 | `12:04:59.247762Z` | 약 19ms |
| c2의 `partitions_assigned` 콜백 | `12:04:59.250071Z` | - |
| c2의 첫 레코드 처리 | `12:04:59.338110Z` | c2 할당 후 약 88ms |
| c1의 첫 레코드 처리 | `12:04:59.850995Z` | c1 할당 후 약 603ms |

c1만 보면 마지막 레코드부터 다음 레코드까지 약 649ms의 간격이 있다. 그러나 c1이 다음에 처리한 이벤트의 생성 시각도 약 `12:04:59.846Z`이므로, c1이 할당 후 기존 레코드를 약 603ms 동안 처리하지 못했다고 볼 근거는 없다. c1이 소유한 파티션 0, 1, 2로 다음 레코드가 들어올 때까지 기다린 시간이 대부분 포함된 것으로 해석한다.

따라서 약 136ms는 모든 컨슈머의 로그를 합쳤을 때 관찰된 두 레코드 출력 사이의 간격이고, 약 649ms는 c1에서 레코드가 출력된 간격이다. 약 19ms도 c1의 파티션 반납 콜백과 할당 콜백 사이의 간격일 뿐 리밸런싱 전체 소요 시간은 아니다. 현재 로그만으로 리밸런싱 자체의 소요 시간을 따로 계산할 수는 없다.

### 해석 및 궁금증

- 같은 컨슈머 그룹의 컨슈머들은 동일한 레코드를 함께 처리하지 않고 파티션을 나누어 맡는다. 그룹 상태가 안정되면 하나의 파티션은 한 멤버에게만 할당되므로 컨슈머를 늘리는 과정에서 일부 파티션의 소유자가 바뀌었다.
- 파티션 6개는 컨슈머가 2개일 때 3개씩, 3개일 때 2개씩 균등하게 나뉘었다. 파티션 수가 컨슈머 수로 나누어떨어지지 않으면 완전히 균등하게 나눌 수 없다.
- Range Assignor의 eager 리밸런싱에서는 c1과 c2가 나중에도 유지할 파티션까지 일단 모두 반납한 후 다시 할당받았다. 실제 소유자가 바뀐 파티션은 컨슈머를 늘릴 때마다 3개였지만 `partitions_revoked` 콜백에는 그보다 많은 파티션이 포함되었다.
- c2 합류 시 c1의 레코드 출력 간격은 약 649ms였지만 모든 컨슈머의 로그에서 레코드 출력이 끊긴 간격은 약 136ms였다. c1의 다음 레코드 자체가 늦게 생성되었으므로 두 값의 차이를 c1의 리밸런싱 지연으로 해석하지 않는다.
- 어떤 조건이 리밸런싱과 처리 재개를 늦추는지는 이번 실험만으로 알 수 없다. 후속 실험에서 종료 방식, 타임아웃, 컨슈머와 파티션 수, 파티션 할당 전략, 리밸런싱 콜백의 작업 시간과 코디네이터까지의 네트워크 지연을 하나씩 바꾸며 비교해 보면 좋을 것 같다.

## Experiment 3. Run More Consumers Than Partitions

### 목적

- 같은 컨슈머 그룹의 컨슈머를 파티션 수보다 하나 많게 되었을 때 각 컨슈머의 파티션 할당을 확인한다.
- 파티션을 할당받지 못한 컨슈머가 레코드를 처리하는지 확인한다.
- 컨슈머 수가 파티션 수를 초과한 뒤에도 처리에 참여하는 컨슈머 수가 늘어나는지 확인한다.

### 명령어

이전 실험의 프로세스와 데이터를 정리한 뒤 Kafka를 다시 시작한다. `application.yml`의 기본값은 변경하지 않고 `create-topic`의 인자로 파티션 수 3을 지정한다.

```bash
docker compose down
docker compose up -d
./gradlew runAdmin --args="create-topic 3"
```

서로 다른 Terminal에서 같은 `group.id`를 사용하는 컨슈머 4개를 순서대로 실행한다. 각 컨슈머의 할당 로그를 확인한 뒤 다음 컨슈머를 실행한다.

```bash
# Terminal 1
./gradlew runConsumer --args="c1"

# Terminal 2
./gradlew runConsumer --args="c2"

# Terminal 3
./gradlew runConsumer --args="c3"

# Terminal 4
./gradlew runConsumer --args="c4"
```

다른 Terminal에서 레코드가 계속 유입되도록 프로듀서를 실행한다.

```bash
# Terminal 5
./gradlew runProducer --args="continuous"
```

컨슈머 그룹이 안정된 뒤 AdminClient와 Kafka CLI로 모든 멤버의 할당을 각각 조회한다.

```bash
./gradlew runAdmin --args="describe-group"
```

### 실제 출력

프로듀서는 파티션 0, 1, 2에 레코드를 전송했다.

![3개 파티션에 레코드를 전송한 프로듀서](images/producer-records-three-partitions.png)

c1은 처음에 파티션 0, 1, 2를 모두 할당받았다. c2와 c3가 합류하면서 담당 파티션이 줄었고, c4 합류 후에도 파티션 0을 다시 할당받아 레코드를 처리했다.

![c1의 리밸런싱과 파티션 0 처리](images/consumer-c1-rebalance-and-partition-0-processing.png)

c2는 처음에 파티션 2를 할당받았고 c3 합류 후 파티션 1로 변경되었다. c4가 합류할 때 파티션 1을 반납한 뒤 같은 파티션을 다시 할당받아 레코드를 처리했다.

![c2의 리밸런싱과 파티션 1 처리](images/consumer-c2-rebalance-and-partition-1-processing.png)

c3는 파티션 2를 할당받았다. c4가 합류할 때 파티션 2를 반납한 뒤 같은 파티션을 다시 할당받아 레코드를 처리했다.

![c3의 리밸런싱과 파티션 2 처리](images/consumer-c3-rebalance-and-partition-2-processing.png)

c4는 컨슈머 그룹에 합류했지만 `partitions_assigned`의 파티션 목록이 비어 있었다. 프로세스는 종료되지 않고 계속 실행 중이었으며, 캡처에는 레코드 처리 로그가 출력되지 않았다.

![c4의 빈 파티션 할당](images/consumer-c4-empty-assignment.png)

AdminClient 조회에서 컨슈머 그룹은 `Stable`, 파티션 할당 전략은 `range`, 멤버 수는 4로 나타났다. c1, c2, c3에는 각각 파티션 0, 1, 2가 할당되었고 c4의 파티션 목록은 비어 있었다.

![멤버 4개인 컨슈머 그룹을 조회한 AdminClient](images/consumer-group-four-members-by-admin-client.png)

### 결과

컨슈머 그룹이 `Stable`인 AdminClient 조회 시점과 각 컨슈머 캡처를 기준으로 기록했다.

| 컨슈머 | 최종 할당 파티션 | 레코드 처리 로그 | 관찰 결과 |
|---|---|---|---|
| c1 | 0 | 있음 | 파티션 0 처리 |
| c2 | 1 | 있음 | 파티션 1 처리 |
| c3 | 2 | 있음 | 파티션 2 처리 |
| c4 | 없음 | 없음 | 프로세스는 실행 중이지만 처리에는 참여하지 않음 |

### 해석 및 궁금증

- 3개 파티션은 c1, c2, c3에 하나씩 할당되었고 네 번째로 합류한 c4에는 파티션이 할당되지 않았다. 컨슈머가 3개에서 4개로 늘어도 레코드 처리에 참여한 컨슈머 수는 3개로 유지되었다.
- c4는 파티션을 할당받지 못했지만 AdminClient에서 컨슈머 그룹의 멤버로 조회되었다. 따라서 파티션 할당이 비어 있다는 것과 컨슈머 그룹에 참여하지 않았다는 것은 다르다.
- c4 프로세스는 빈 파티션 할당을 받은 뒤에도 실행 중이었다. 다만 현재 로그에는 `poll()` 호출 자체를 기록하지 않으므로 `poll()`이 계속 호출되는지는 캡처만으로 확인할 수 없다.
- c4가 합류할 때 c1, c2, c3는 각각 담당하던 파티션을 반납한 뒤 같은 파티션을 다시 할당받았다. 최종 파티션 소유자는 바뀌지 않았지만 Range Assignor의 eager 리밸런싱은 발생했으므로, 파티션을 할당받지 못한 컨슈머를 추가하는 것도 비용이 없지는 않았다.

## Experiment 4. Graceful Close vs Abrupt Termination

### 목적

- 같은 조건에서 c2를 SIGTERM으로 정상 종료한 경우와 `kill -9`로 강제 종료한 경우를 각각 실행한다.
- 종료 대상인 c2에서 종료 요청, 파티션 반납과 `consumer.close()` 시작·완료를 구분해 기록한다.
- 남은 c1의 파티션 반납·할당 콜백 시각을 비교해 c2 종료부터 재할당까지 걸린 시간을 확인한다.
- 종료 전후의 컨슈머 그룹 멤버 수와 파티션 할당을 AdminClient로 확인한다.

### 명령어

먼저 컨슈머가 시작될 때 PID를 출력하고, SIGTERM을 받으면 `poll()`을 중단한 뒤 컨슈머 실행 스레드에서 `consumer.close()`를 호출하도록 구현한다. c2에서는 종료 요청, 파티션 반납과 `close()` 시작·완료 시각을, c1에서는 파티션 반납·할당 시각을 로그로 남긴다.

첫 번째 실행에서는 Kafka와 6개 파티션 토픽을 준비한다.

```bash
docker compose down
docker compose up -d
./gradlew runAdmin --args="create-topic 6"
```

서로 다른 터미널에서 c1, c2와 지속 프로듀서를 실행한다. 두 컨슈머의 최초 할당과 로그에 출력된 c2 PID를 확인한다.

```bash
# Terminal 1
./gradlew runConsumer --args="c1"

# Terminal 2
./gradlew runConsumer --args="c2"

# Terminal 3
./gradlew runProducer --args="continuous"
```

컨슈머 그룹이 `Stable`인 상태를 조회한 뒤 c2에 SIGTERM을 보내고, c1의 재할당이 끝난 상태를 다시 조회한다.

```bash
./gradlew runAdmin --args="describe-group"
kill -TERM <c2-pid>
./gradlew runAdmin --args="describe-group"
```

첫 번째 실행의 로그를 보존한 뒤 모든 프로세스와 Kafka를 종료한다. 두 번째 실행에서는 Kafka와 토픽을 다시 만들고 c1, c2와 지속 프로듀서를 같은 순서로 실행한다.

```bash
docker compose down
docker compose up -d
./gradlew runAdmin --args="create-topic 6"

./gradlew runConsumer --args="c1"
./gradlew runConsumer --args="c2"
./gradlew runProducer --args="continuous"
```

강제 종료 시각은 c2 내부에 남길 수 없으므로 외부 터미널에서 UTC 시각을 출력한 직후 c2에 SIGKILL을 보낸다. c1의 재할당이 끝나면 컨슈머 그룹을 다시 조회한다.

```bash
python3 -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())' \
  && kill -9 <c2-pid>
./gradlew runAdmin --args="describe-group"
```

### 실제 출력

SIGTERM 실행에서 c1은 처음 6개 파티션을 모두 할당받았고, c2가 합류한 뒤 파티션 0, 1, 2를 할당받았다.

![SIGTERM 실행 전 c1의 파티션 할당](images/consumer-c1-assignment-before-sigterm.png)

c2는 PID 5877로 시작했고 파티션 3, 4, 5를 할당받았다.

![SIGTERM 실행 전 c2의 파티션 할당](images/consumer-c2-assignment-before-sigterm.png)

종료 전 AdminClient 조회에서 그룹은 `Stable`, 멤버 수는 2였으며 c1과 c2가 파티션을 3개씩 나누어 가지고 있었다.

![SIGTERM 실행 전 컨슈머 그룹](images/consumer-group-before-sigterm.png)

c2는 SIGTERM 이후 종료 요청을 기록하고 `consumer.close()`를 시작했다. 파티션 3, 4, 5의 반납 콜백이 호출된 뒤 0.165994초 만에 Close가 완료되었다. Gradle은 시그널로 종료된 JavaExec 프로세스를 작업 실패로 표시했다.

![SIGTERM 이후 c2의 정상 종료](images/consumer-c2-graceful-close-after-sigterm.png)

c1은 파티션 0, 1, 2를 반납한 뒤 6개 파티션을 모두 다시 할당받았다. 캡처에는 리밸런싱 전 마지막 레코드와 리밸런싱 후 첫 레코드도 포함되어 있다.

![SIGTERM 이후 c1의 파티션 재할당](images/consumer-c1-rebalance-after-sigterm.png)

종료 후 AdminClient 조회에서 그룹은 `Stable`, 멤버 수는 1이었으며 c1이 6개 파티션을 모두 가지고 있었다.

![SIGTERM 실행 후 컨슈머 그룹](images/consumer-group-after-sigterm.png)

SIGKILL 실행에서는 외부 터미널에서 UTC 시각 `2026-10-10T11:37:12.495777+00:00`을 출력한 직후 PID 9247인 c2에 `kill -9`를 실행했다.

![UTC 시각을 기록한 SIGKILL 명령](images/sigkill-command-with-utc-timestamp.png)

c2에서는 마지막 레코드 처리 로그 직후 프로세스가 exit code 137로 종료되었다. SIGTERM 실행에서 확인한 종료 요청, 파티션 반납과 Close 시작·완료 로그는 남지 않았다.

![SIGKILL 이후 c2의 강제 종료](images/consumer-c2-abrupt-termination-after-sigkill.png)

c1은 SIGKILL 시각으로부터 약 44초 뒤 파티션 0, 1, 2를 반납하고 6개 파티션을 모두 다시 할당받았다. 캡처에는 리밸런싱 전 마지막 레코드와 리밸런싱 후 첫 레코드도 포함되어 있다.

![SIGKILL 이후 c1의 파티션 재할당](images/consumer-c1-rebalance-after-sigkill.png)

AdminClient 조회에서 종료 전에는 그룹이 `Stable`, 멤버 수가 2였고 c1과 c2가 파티션을 3개씩 나누어 가지고 있었다. 종료 후에는 그룹이 다시 `Stable`이 되었고, 남은 c1이 6개 파티션을 모두 가지고 있었다.

![SIGKILL 실행 전후 컨슈머 그룹](images/consumer-group-before-and-after-sigkill.png)

### 결과

SIGTERM의 종료 기준은 c2의 `shutdown_requested`, SIGKILL의 종료 기준은 외부 터미널에 기록할 UTC 시각으로 삼는다. 종료 시점은 두 실행 모두 c1의 `partitions_assigned`로 맞춘다.

| 종료 방식 | 종료 기준 시각 | c1 파티션 반납 시각 | c1 파티션 할당 시각 | 종료 기준→할당 간격 | c2에서 관찰한 마지막 이벤트 |
|---|---|---|---|---:|---|
| SIGTERM | 2026-10-10T10:22:57.380177Z | 2026-10-10T10:23:00.386960Z | 2026-10-10T10:23:00.393714Z | 3.013537초 | `consumer_close_completed` |
| SIGKILL | 2026-10-10T11:37:12.495777Z | 2026-10-10T11:37:56.643790Z | 2026-10-10T11:37:56.650906Z | 44.155129초 | `processedDate` 2026-10-10T11:37:12.347093Z, 종료 로그 없음 |

종료 전후 파티션 할당을 기록한다.

| 종료 방식 | 시점 | c1 할당 파티션 | c2 할당 파티션 | 그룹 멤버 수 | 그룹 상태 |
|---|---|---|---|---:|---|
| SIGTERM | 종료 전 | 0, 1, 2 | 3, 4, 5 | 2 | Stable |
| SIGTERM | 종료 후 | 0, 1, 2, 3, 4, 5 | - | 1 | Stable |
| SIGKILL | 종료 전 | 0, 1, 2 | 3, 4, 5 | 2 | Stable |
| SIGKILL | 종료 후 | 0, 1, 2, 3, 4, 5 | - | 1 | Stable |

c1의 레코드 로그에서 관찰한 출력 간격은 리밸런싱 시간과 분리해 기록한다.

| 종료 방식 | 리밸런싱 전 마지막 처리 시각 | 리밸런싱 후 첫 처리 시각 | 레코드 출력 간격 |
|---|---|---|---:|
| SIGTERM | 2026-10-10T10:22:59.914377Z | 2026-10-10T10:23:00.436466Z | 0.522089초 |
| SIGKILL | 2026-10-10T11:37:56.158343Z | 2026-10-10T11:37:56.678010Z | 0.519667초 |

### 해석 및 궁금증

- SIGTERM에서는 c2가 `consumer.close()`를 실행하면서 파티션 반납과 Close 완료를 기록했고, 약 3.014초 뒤 c1의 재할당이 완료되었다. SIGKILL에서는 c2의 종료 로그 없이 프로세스가 끝났고, 기본 세션 타임아웃 45초에 가까운 약 44.155초 뒤 c1의 재할당이 완료되었다.
- 이외의 작업은 차이가 없었던 것으로 확인된다.

## Experiment 5. Heartbeat, Session Timeout and Network Failure

### 목적

- `heartbeat.interval.ms`를 2초로 고정하고 `session.timeout.ms`를 6초와 15초로 바꿔 c2를 SIGKILL로 종료한다.
- 장애 주입부터 남은 c1이 파티션 반납과 재할당을 관찰할 때까지 걸린 시간을 구분해 측정한다.
- Heartbeat 주기와 Session Timeout이 장애 감지 과정에서 서로 다른 역할을 하는지 확인한다.
- 후속 실행에서 Network 단절을 같은 기준으로 비교한다.

### 명령어

첫 번째 실행에서는 Consumer 설정에 Heartbeat 2초와 Session Timeout 6초를 적용했다.

```yaml
kafka:
  consumer:
    heartbeat-interval-ms: 2000
    session-timeout-ms: 6000
```

Kafka와 6개 파티션 토픽을 준비했다.

```bash
docker compose down
docker compose up -d
./gradlew runAdmin --args="create-topic 6"
```

서로 다른 터미널에서 c1, c2와 지속 프로듀서를 실행했다.

```bash
# Terminal 1
./gradlew runConsumer --args="c1"

# Terminal 2
./gradlew runConsumer --args="c2"

# Terminal 3
./gradlew runProducer --args="continuous"
```

두 컨슈머의 파티션 할당이 끝난 뒤 컨슈머 그룹이 `Stable`인지 확인했다.

```bash
./gradlew runAdmin --args="describe-group"
```

외부 터미널에서 UTC 시각을 출력한 직후 c2의 Java Process에 SIGKILL을 보냈다. c1의 재할당이 끝난 뒤 컨슈머 그룹을 다시 조회했다.

```bash
python3 -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())' \
  && kill -9 83365

./gradlew runAdmin --args="describe-group"
```

첫 번째 실행의 로그를 보존한 뒤 모든 프로세스와 Kafka를 종료했다. 두 번째 실행에서는 Heartbeat를 2초로 유지하고 Session Timeout만 15초로 변경했다.

```yaml
kafka:
  consumer:
    heartbeat-interval-ms: 2000
    session-timeout-ms: 15000
```

Kafka와 토픽을 다시 만들고 c1, c2와 지속 프로듀서를 같은 순서로 실행했다. 컨슈머 그룹이 `Stable`이고 멤버가 2개인 상태를 확인한 뒤 c2에 SIGKILL을 보냈다.

```bash
docker compose down
docker compose up -d
./gradlew runAdmin --args="create-topic 6"

./gradlew runConsumer --args="c1"
./gradlew runConsumer --args="c2"
./gradlew runProducer --args="continuous"

./gradlew runAdmin --args="describe-group"

python3 -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())' \
  && kill -9 84579

./gradlew runAdmin --args="describe-group"
```

Toxiproxy는 클라이언트와 서버 사이에서 TCP 연결을 전달하면서 연결 차단, 지연과 대역폭 제한 같은 Network 장애를 주입하는 도구다. Kafka 구성 요소는 아니며 이번 Lab에서 c2의 연결만 선택적으로 끊기 위한 실험 도구로 사용한다.

Kafka 컨테이너나 공통 Docker Network를 중단하면 c1, Producer와 AdminClient도 함께 영향을 받는다. c2 프로세스를 실행한 상태로 유지하면서 c2와 Kafka 사이의 통신만 차단하기 위해 Kafka에 직접 연결하는 `DIRECT` listener와 Toxiproxy를 통과하는 `PROXY` listener를 분리했다. Toxiproxy의 제어 API와 c2 접속 포트는 호스트의 loopback interface에만 공개했다.

```yaml
services:
  kafka:
    ports:
      - "9092:9092"
    environment:
      KAFKA_LISTENERS: DIRECT://:9092,PROXY://:29093,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: DIRECT://localhost:9092,PROXY://localhost:29092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: DIRECT
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,DIRECT:PLAINTEXT,PROXY:PLAINTEXT

  toxiproxy:
    image: ghcr.io/shopify/toxiproxy:2.13.0
    ports:
      - "127.0.0.1:8474:8474"
      - "127.0.0.1:29092:29092"
    depends_on:
      - kafka
```

두 컨슈머의 연결 경로는 다음과 같이 분리한다.

```text
c1 -> localhost:9092 -> Kafka DIRECT listener
c2 -> localhost:29092 -> Toxiproxy -> kafka:29093 -> Kafka PROXY listener
```

Network 단절 실행 전 Compose 설정을 검증하고, Toxiproxy에 c2 전용 proxy를 생성한다. `listen`은 c2가 접속하는 Toxiproxy 포트이고 `upstream`은 Docker Network 안에서 Toxiproxy가 접속하는 Kafka service와 포트다.

```bash
docker compose config
docker compose down
docker compose up -d

curl -fsS \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "kafka-c2",
    "listen": "0.0.0.0:29092",
    "upstream": "kafka:29093",
    "enabled": true
  }' \
  http://localhost:8474/proxies

curl -fsS http://localhost:8474/proxies/kafka-c2
./gradlew runAdmin --args="create-topic 6"
```

c1은 기존 Kafka Port에 직접 연결하고 c2만 Toxiproxy Port를 Bootstrap Server로 사용한다. Producer와 AdminClient도 기존 직접 연결을 유지한다.

```bash
# Terminal 1
./gradlew runConsumer --args="c1"

# Terminal 2
./gradlew runConsumer --args="c2 localhost:29092"

# Terminal 3
./gradlew runProducer --args="continuous"

# 별도 Terminal
./gradlew runAdmin --args="describe-group"
```

Network 단절 시각을 기록한 뒤 c2 전용 proxy만 비활성화한다. c1이 6개 파티션을 다시 할당받은 것을 확인할 때까지 proxy를 비활성화한 상태로 유지한다.

```bash
python3 -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())' \
  && curl -fsS \
    -X PATCH \
    -H 'Content-Type: application/json' \
    -d '{"enabled":false}' \
    http://localhost:8474/proxies/kafka-c2

curl -fsS http://localhost:8474/proxies/kafka-c2
ps -p <c2-java-pid> -o pid=,stat=,command=
./gradlew runAdmin --args="describe-group"
```

c1의 재할당 결과를 보존한 뒤 proxy를 다시 활성화한다. c2가 재가입한 뒤 컨슈머 그룹을 다시 조회한다.

```bash
python3 -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())' \
  && curl -fsS \
    -X PATCH \
    -H 'Content-Type: application/json' \
    -d '{"enabled":true}' \
    http://localhost:8474/proxies/kafka-c2

./gradlew runAdmin --args="describe-group"
```

### 실제 출력

외부 터미널에 UTC 시각 `2026-10-24T11:18:58.449321+00:00`을 기록한 직후 PID 83365인 c2에 SIGKILL을 보냈다.

![Session Timeout 6초 조건에서 c2에 SIGKILL을 보낸 시각](images/consumer-c2-sigkill-with-session-timeout-6s.png)

c1은 장애 주입 시각으로부터 약 6.890초 뒤 기존 파티션 0, 1, 2를 반납했다. 약 9ms 뒤 파티션 0부터 5까지를 모두 다시 할당받았다.

![c2 SIGKILL 이후 c1의 파티션 반납과 재할당](images/consumer-c1-rebalance-after-session-timeout-6s.png)

재할당 후 AdminClient 조회에서 컨슈머 그룹은 `Stable`, 멤버 수는 1이었으며 c1이 파티션 0부터 5까지 모두 소유했다.

![c2 SIGKILL 이후 하나의 멤버만 남은 컨슈머 그룹](images/consumer-group-after-session-timeout-6s.png)

Session Timeout 15초 실행에서 장애 주입 전 컨슈머 그룹은 `Stable`이었고, c1은 파티션 0, 1, 2를, c2는 파티션 3, 4, 5를 소유했다.

![Session Timeout 15초 실행에서 SIGKILL 전 컨슈머 그룹](images/consumer-group-before-sigkill-session-timeout-15s.png)

외부 터미널에 UTC 시각 `2026-10-24T11:31:54.631760+00:00`을 기록한 직후 PID 84579인 c2에 SIGKILL을 보냈다.

![Session Timeout 15초 조건에서 c2에 SIGKILL을 보낸 시각](images/consumer-c2-sigkill-with-session-timeout-15s.png)

c1은 장애 주입 시각으로부터 약 14.072초 뒤 기존 파티션 0, 1, 2를 반납했다. 약 9ms 뒤 파티션 0부터 5까지를 모두 다시 할당받았다.

![Session Timeout 15초 조건에서 c2 SIGKILL 이후 c1의 파티션 반납과 재할당](images/consumer-c1-rebalance-after-session-timeout-15s.png)

재할당 후 AdminClient 조회에서 컨슈머 그룹은 다시 `Stable`이 되었고, 남은 c1이 파티션 6개를 모두 소유했다.

![Session Timeout 15초 조건에서 c2 SIGKILL 이후 컨슈머 그룹](images/consumer-group-after-session-timeout-15s.png)

Network 단절 실행에서는 `kafka-c2` proxy가 `localhost:29092`에서 요청을 받고 Kafka의 `kafka:29093`으로 전달하도록 생성했다. 생성 결과에서 proxy는 `enabled=true`였다.

![c2 전용 Toxiproxy 생성](images/toxiproxy-kafka-c2-created.png)

c1은 PID 86127로 시작해 `localhost:9092`에 직접 연결했고, Heartbeat 2초와 Session Timeout 6초를 적용했다. c2 합류 전에는 파티션 6개를 모두 소유했고 합류 후에는 파티션 0, 1, 2를 할당받았다.

![Network 단절 전 DIRECT listener에 연결한 c1](images/consumer-c1-direct-listener-before-network-disconnection.png)

c2는 PID 86155로 시작해 `localhost:29092`의 Toxiproxy를 통과했고, 같은 Heartbeat와 Session Timeout을 적용했다. c2는 파티션 3, 4, 5를 할당받았다.

![Network 단절 전 PROXY listener에 연결한 c2](images/consumer-c2-proxy-listener-before-network-disconnection.png)

장애 주입 전 AdminClient 조회에서 컨슈머 그룹은 `Stable`, 멤버 수는 2였으며 c1과 c2가 파티션을 3개씩 소유했다. c1은 호스트에서 직접 접속해 `/192.168.65.1`로, c2는 Toxiproxy 컨테이너를 통해 접속해 `/172.18.0.3`으로 조회되었다.

![c2 Network 단절 전 컨슈머 그룹](images/consumer-group-before-c2-network-disconnection.png)

UTC 시각 `2026-10-24T12:07:33.975166+00:00`을 기록한 직후 `kafka-c2` proxy를 비활성화했다. 조회 결과는 `enabled=false`였지만 PID 86155인 c2 Java 프로세스는 `S` 상태로 계속 실행 중이었다. 직후 AdminClient 조회에서는 그룹이 `PreparingRebalance`, 멤버 수가 1로 나타났으며 c1의 파티션은 아직 비어 있었다.

![c2 Network 단절과 실행 중인 c2 프로세스](images/consumer-c2-network-disconnected-process-alive.png)

c1은 Network 단절 시각으로부터 약 7.494초 뒤 파티션 0, 1, 2를 반납했고 약 8ms 뒤 파티션 0부터 5까지를 모두 다시 할당받았다.

![c2 Network 단절 이후 c1의 파티션 반납과 재할당](images/consumer-c1-rebalance-after-c2-network-disconnection.png)

UTC 시각 `2026-10-24T12:12:09.922127+00:00`을 기록한 직후 proxy를 다시 활성화했다. 이후 AdminClient 조회에서 그룹은 `Stable`, 멤버 수는 2였으며 c1은 파티션 0, 1, 2를, c2는 파티션 3, 4, 5를 다시 소유했다.

![c2 Network 복구와 컨슈머 그룹 재가입](images/consumer-c2-network-recovery-and-group-rejoined.png)

c1에서도 c2 재가입으로 파티션 6개를 반납한 뒤 파티션 0, 1, 2를 다시 할당받는 두 번째 리밸런싱을 확인했다.

![c2 Network 복구 이후 c1의 두 번째 리밸런싱](images/consumer-c1-rebalance-after-c2-network-recovery.png)

### 결과

먼저 핵심 비교만 요약한다.

| 확인한 질문 | 관찰 결과 | 이번 실행에서 내릴 수 있는 판단 |
|---|---|---|
| Session Timeout을 늘리면 감지가 늦어지는가? | SIGKILL 전체 재할당이 6초 설정에서 6.899초, 15초 설정에서 14.080초였다. | 같은 Heartbeat에서 Session Timeout이 길수록 재할당도 늦어졌다. |
| SIGKILL과 Network 단절의 감지 시간은 다른가? | 같은 6초 설정에서 각각 6.899초와 7.501초였다. | 한 번씩 측정한 0.602초 차이만으로 장애 종류의 영향이라고 판단할 수 없다. |
| Network가 복구되면 어떻게 되는가? | 복구 후 1.715초 안에 c1의 재할당이 끝나고 그룹이 다시 2명이 되었다. | 살아 있던 c2가 재가입하면서 두 번째 리밸런싱이 발생했다. |

장애 주입과 복구 시각은 외부 터미널에서 기록하고, 리밸런싱 시작과 완료는 c1의 콜백 시각으로 관찰했다.

| 기준 | 의미 |
|---|---|
| `T0` | c2에 SIGKILL을 보내거나 c2 전용 proxy를 비활성화한 시각 |
| `T1` | c1의 `partitions_revoked` 콜백 시각 |
| `T2` | c1의 `partitions_assigned` 콜백 시각 |
| `T3` | c2 전용 proxy를 다시 활성화한 시각 |
| `T4` | c2 재가입 후 c1의 `partitions_revoked` 콜백 시각 |
| `T5` | c2 재가입 후 c1의 `partitions_assigned` 콜백 시각 |

장애 조건별 핵심 측정값은 다음과 같다.

| 장애 조건 | Heartbeat | Session Timeout | c1에서 관찰한 장애 감지 `T1-T0` | 콜백에서 관찰한 재할당 `T2-T1` | 장애 주입부터 재할당 `T2-T0` | 장애 후 c2 프로세스 |
|---|---:|---:|---:|---:|---:|---|
| SIGKILL | 2000ms | 6000ms | 6.889946초 | 0.009451초 | 6.899397초 | 종료 |
| SIGKILL | 2000ms | 15000ms | 14.071643초 | 0.008774초 | 14.080417초 | 종료 |
| Network 단절 | 2000ms | 6000ms | 7.493580초 | 0.007859초 | 7.501439초 | 실행 중 |

원본 시각은 계산을 다시 검증할 수 있도록 별도로 남긴다.

| 장애 조건 | Session Timeout | T0: 장애 주입 | T1: 파티션 반납 | T2: 파티션 할당 |
|---|---:|---|---|---|
| SIGKILL | 6000ms | 2026-10-24T11:18:58.449321Z | 2026-10-24T11:19:05.339267Z | 2026-10-24T11:19:05.348718Z |
| SIGKILL | 15000ms | 2026-10-24T11:31:54.631760Z | 2026-10-24T11:32:08.703403Z | 2026-10-24T11:32:08.712177Z |
| Network 단절 | 6000ms | 2026-10-24T12:07:33.975166Z | 2026-10-24T12:07:41.468746Z | 2026-10-24T12:07:41.476605Z |

Network 단절과 복구 과정의 상태 변화는 다음과 같다.

| 시점 | Proxy | c2 프로세스 | 그룹 상태 | 멤버 수 | c1 파티션 | c2 파티션 |
|---|---|---|---|---:|---|---|
| 단절 전 | `enabled=true` | 실행 중 | Stable | 2 | 0, 1, 2 | 3, 4, 5 |
| 단절 직후 조회 | `enabled=false` | 실행 중 | PreparingRebalance | 1 | 할당 전 | - |
| Session 만료 후 c1 콜백 | `enabled=false` | 실행 중 | 조회하지 않음 | 조회하지 않음 | 0, 1, 2, 3, 4, 5 | - |
| Network 복구 후 | `enabled=true` | 실행 중 | Stable | 2 | 0, 1, 2 | 3, 4, 5 |

Network 복구부터 c2 재가입으로 발생한 두 번째 리밸런싱까지의 측정값은 다음과 같다.

| T3: Network 복구 | T4: c1 파티션 반납 | T5: c1 파티션 할당 | 재가입 관찰 `T4-T3` | 콜백에서 관찰한 재할당 `T5-T4` | 복구 후 재할당 `T5-T3` |
|---|---|---|---:|---:|---:|
| 2026-10-24T12:12:09.922127Z | 2026-10-24T12:12:11.627184Z | 2026-10-24T12:12:11.637034Z | 1.705057초 | 0.009850초 | 1.714907초 |

### 해석 및 궁금증

#### 설정값의 역할

| 설정 | 설정 주체 | 의미 | 이번 실험에서 확인한 점                                                         |
|---|---|---|-----------------------------------------------------------------------|
| `heartbeat.interval.ms` | 컨슈머 | 브로커에 생존 신호를 보내는 주기 | 2초마다 보내더라도 한 번 누락됐다고 바로 제거되지는 않았다.                                    |
| `session.timeout.ms` | 컨슈머 | 마지막으로 성공한 Heartbeat 이후 멤버 제거까지 허용하는 시간 | 6초에서 15초로 늘리자 장애 감지 관찰값도 약 6.890초에서 14.072초로 늘었다.                     |
| `group.min.session.timeout.ms` | 브로커 | 컨슈머가 요청할 수 있는 Session Timeout의 최솟값 | 이 값보다 작으면 브로커가 그룹 가입을 거부한다. 현재 구현에서는 `InvalidSessionTimeoutException`이 전파되어 컨슈머 애플리케이션이 종료된다. |

#### Session Timeout과 전체 복구 시간

- Heartbeat를 2초로 고정하고 Session Timeout만 6초에서 15초로 늘렸을 때 SIGKILL 후 전체 재할당 시간은 약 6.899초에서 14.080초로 증가했다.
- Experiment 4의 기본 Session Timeout 45초 조건에서는 같은 SIGKILL 후 약 44.155초가 걸렸다. 세 실행 모두 Session Timeout이 길수록 전체 재할당도 길어지는 방향을 보였다.
- 세 장애 실행에서 파티션 반납부터 재할당까지의 콜백 구간은 약 8~9ms였다. 전체 시간의 대부분은 파티션 할당 자체보다 Session 만료를 기다리는 구간에서 발생했다.

#### SIGKILL과 Network 단절

- 같은 Heartbeat 2초와 Session Timeout 6초에서 전체 재할당 시간은 SIGKILL 약 6.899초, Network 단절 약 7.501초였다.
- 차이는 약 0.602초였지만 각 조건을 한 번씩만 실행했으므로 장애 종류가 감지 시간을 다르게 만들었다고 일반화할 수 없다.
- 브로커에는 두 조건 모두 c2의 Heartbeat가 도착하지 않았다. Heartbeat 부재만으로 c2 프로세스 종료와 Network 단절의 원인을 구분할 수는 없었다.
- Network 단절에서는 c2 프로세스가 계속 실행되었다. Proxy를 복구하자 같은 c2가 다시 가입해 두 번째 리밸런싱이 발생했다. SIGKILL에서는 c2 프로세스를 다시 시작하지 않아 재가입이 발생하지 않았다.

#### Network 단절 구성에서 확인한 개념

| 구성 | 의미 | 이번 구성에서의 값 |
|---|---|---|
| `KAFKA_LISTENERS` | Kafka가 실제 연결을 받는 주소와 포트 | `DIRECT://:9092`, `PROXY://:29093`, `CONTROLLER://:9093` |
| `KAFKA_ADVERTISED_LISTENERS` | Bootstrap 이후 클라이언트에 알려주는 재접속 주소 | c1은 `localhost:9092`, c2는 `localhost:29092` |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | listener 이름을 실제 보안 프로토콜에 연결 | `DIRECT`와 `PROXY` 모두 `PLAINTEXT` |
| Toxiproxy | TCP 연결을 중계하고 장애를 주입 | `localhost:29092`에서 받아 `kafka:29093`으로 전달 |

- 변경 전 `PLAINTEXT://:9092`의 앞부분은 listener 이름이었다. `PLAINTEXT:PLAINTEXT` 매핑에서는 왼쪽이 listener 이름, 오른쪽이 암호화와 인증을 사용하지 않는 보안 프로토콜이었다.
- 현재는 경로를 구분하기 위해 listener 이름을 `DIRECT`와 `PROXY`로 바꿨을 뿐, 두 경로 모두 `PLAINTEXT`를 사용하므로 보안 방식은 바뀌지 않았다.
- Kafka 클라이언트는 `bootstrap.servers`에만 계속 연결하지 않고 브로커가 광고한 주소로 다시 연결한다. 따라서 c2가 Toxiproxy를 우회하지 않도록 `PROXY`의 광고 주소도 `localhost:29092`로 설정했다.
- `CONTROLLER://:9093`은 KRaft Controller 통신용이고 이번 컨슈머 Network 단절 대상이 아니다. `depends_on`도 컨테이너 시작 순서만 보장하며 Kafka의 준비 완료까지 보장하지 않는다.

#### 측정 한계와 남은 질문

| 항목 | 한계 또는 다음 확인 사항 |
|---|---|
| 장애 감지 시각 | `T1`은 브로커 내부의 Session 만료 시각이 아니라 c1이 파티션 반납 콜백을 실행한 시각이다. |
| Session Timeout과 측정값 차이 | `T0`가 마지막 Heartbeat 성공 시각과 일치하지 않아 측정값이 설정값과 정확히 같지 않다. |
| 장애 종류 비교 | 각 조건을 한 번만 실행했으므로 반복 실행 결과의 분포와 편차는 확인하지 않았다. |
| Network 단절 후 Group 상태 | c1의 6개 파티션 할당은 콜백으로 확인했지만 그 시점의 최종 `Stable` 상태를 AdminClient로 조회하지 않았다. |
| 짧은 Session Timeout의 위험 | 정상 컨슈머가 일시적인 Network 지연, 브로커 지연 또는 긴 JVM pause 때문에 제거되는 조건은 아직 재현하지 않았다. |
| 장애 원인 구분 | 브로커가 관찰하는 Heartbeat 부재만으로 프로세스 종료, Network 단절과 일시적인 지연을 구분할 수 있는지 추가 로그와 메트릭으로 확인할 필요가 있다. |

## Troubleshooting

### `createTopics()`를 호출해도 중복 생성이 모두 성공한 것처럼 보임

- 증상: `admin.createTopics(listOf(newTopic))`만 호출했을 때 같은 명령을 반복해도 Gradle 실행이 성공으로 종료되었다.
- 원인: `createTopics()`는 비동기 결과인 `CreateTopicsResult`를 반환한다. 반환된 Future의 완료 결과를 확인하지 않아 Broker가 반환한 실패가 실행 스레드로 전파되지 않았다.
- 해결: `admin.createTopics(...).all().get()`으로 전체 Topic 생성 결과를 기다렸다.
- 확인: 첫 번째 생성은 성공했고, 같은 Topic을 다시 생성했을 때 `ExecutionException`의 원인으로 `TopicExistsException`이 출력되며 실행이 실패했다.
- 재발 방지: AdminClient의 변경 작업은 요청 전송 성공과 Broker 반영 성공을 구분하고, 완료 Future와 Topic별 오류를 반드시 확인한다.

![Topic 최초 생성과 중복 생성 오류](images/order-events-topic-created-and-duplicate-rejected.png)

### SLF4J Logger Binder 경고

- 증상: AdminClient 실행마다 `Failed to load class org.slf4j.impl.StaticLoggerBinder`와 NOP Logger 경고가 출력되었다.
- 원인: Kafka Client가 사용하는 SLF4J API에 대응하는 Logging 구현체를 현재 Lab의 Runtime Dependency로 추가하지 않았다.
- 영향: Kafka Admin 요청과 Topic/Consumer Group 조회 자체는 정상적으로 완료되었지만 Kafka Client 내부 로그는 출력되지 않는다.
- 대응: Experiment 1에서는 경고와 기능 실패를 구분해 기록하고, 현재 실험에 필요하지 않은 Logging Dependency는 미리 추가하지 않는다.
- 재발 방지: 이후 Kafka Client 내부 로그가 실제 관찰 대상이 되는 Experiment에서 SLF4J API 버전과 호환되는 구현체를 명시적으로 선택한다.

### Experiment 4의 SIGTERM 실행에서 정상 종료 로그 없이 세션 타임아웃 뒤 재할당됨

- 증상: `kill -TERM` 이후 c2의 종료 요청과 `consumer.close()` 로그가 없었고, c1은 약 45초 뒤 파티션을 재할당받았다.
- 원인: Experiment 4의 첫 번째 실행에서 `ps -ef | grep c2`에 표시된 Gradle Wrapper PID를 종료했다. 실제 c2는 Gradle이 실행한 별도의 `OrderEventConsumerKt c2` JVM이었다.
- 해결: 컨슈머가 시작할 때 자신의 PID를 출력하도록 하고 그 PID에 SIGTERM을 보낸다. SIGTERM 수신 시 `poll()`을 중단하고 컨슈머 실행 스레드에서 `consumer.close()`를 호출하는 종료 처리도 먼저 구현한다.
- 재발 방지: 종료 전에 PID의 명령이 `lab.kafka.OrderEventConsumerKt c2`인지 확인하며, Gradle Wrapper나 Gradle Daemon PID를 사용하지 않는다.

### Experiment 5 Network 단절 구성 점검

- c2의 시작 로그에서 `bootstrapServers=localhost:29092`인지 확인한다. `localhost:9092`라면 c2도 Kafka에 직접 연결하므로 Toxiproxy를 비활성화해도 영향을 받지 않는다.
- Kafka 클라이언트는 Bootstrap Server에서 브로커 메타데이터를 받은 뒤 광고된 주소로 다시 연결한다. `PROXY`의 `advertised.listeners`가 `localhost:9092`를 가리키면 c2가 첫 연결 이후 Toxiproxy를 우회하므로 `PROXY://localhost:29092`로 유지한다.
- Toxiproxy 컨테이너 안에서 `localhost`는 Kafka가 아니라 Toxiproxy 자신이다. Proxy의 `upstream`은 Compose service 이름을 사용해 `kafka:29093`으로 설정한다.
- Toxiproxy를 비활성화했을 때 c1까지 끊긴다면 c1이 `localhost:29092`를 사용하고 있거나 Kafka 컨테이너 또는 공통 Docker Network를 중단한 것이다. c1, Producer와 AdminClient는 `localhost:9092`의 `DIRECT` 경로를 유지한다.
- `depends_on`은 컨테이너 시작 순서만 보장한다. Kafka 준비 전에 Topic 생성이 실패하면 `docker compose ps`와 `docker compose logs kafka`로 브로커 상태를 확인한 뒤 명령을 다시 실행한다.
- Toxiproxy API는 인증 없이 Proxy 상태를 변경할 수 있으므로 `8474`와 c2 접속용 `29092`는 `127.0.0.1`에만 bind한다.

## Key Takeaways
