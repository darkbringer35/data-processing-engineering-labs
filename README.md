# data-processing-engineering-labs

다양한 데이터 처리 기술을 직접 구현하고 실험하며,
데이터가 수집되고, 이동하고, 처리되고, 저장되는 과정을 학습하기 위한 실습 레포지토리입니다.

Kafka와 Flink를 중심으로 시작하지만,
CDC, Batch Processing, Stream Processing, Data Lake,
OLAP, Data Quality, Recommendation Pipeline 등
실제 데이터 엔지니어링에서 마주치는 문제를 단계적으로 다룹니다.

단순한 API 사용법이나 예제 구현보다

- 데이터 처리 시스템의 내부 동작 관찰
- 성능 병목 및 장애 상황 재현
- 데이터 정합성과 처리 보장 검증
- 요구사항에 따른 데이터 파이프라인 설계
- 실제 데이터와 비즈니스 시나리오 기반 문제 해결

을 목표로 합니다.

## Goals

- 대용량 데이터의 수집, 전달, 처리, 저장 과정 이해
- Batch / Stream Processing의 특성과 Trade-off 이해
- Kafka, Flink 등 분산 데이터 처리 기술의 동작 원리 실험
- CDC, Event-driven Pipeline, Data Lake, OLAP 구조 구현
- 장애, 지연, 중복, 유실, Backpressure 등 실제 문제 재현
- 데이터 정합성과 처리 보장 방식 검증
- 실제 비즈니스 요구사항에 맞는 데이터 파이프라인 설계

## Lab

| Lab | Topic | What I Learn |
|---|---|---|
| 01 | Kafka Basic Pipeline | Producer → Kafka → Consumer 기본 흐름과 offset/lag |