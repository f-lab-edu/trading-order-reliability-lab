# Milestone Progress

이 문서는 새 Codex thread가 사용자의 별도 설명 없이 현재 개발 진행 상황을 파악하기 위한 handoff 문서다.

개발 작업을 시작하는 agent는 반드시 다음 순서로 읽는다.

1. `docs/00-index.md`
2. 이 문서
3. `docs/13-development-milestones.md`
4. active milestone에 연결된 최신 `local-notes/ai-work-log/*.md`
5. active milestone 관련 설계문서

이 문서는 큰 milestone 상태만 기록한다. 세부 피드백, 문제 카드, 테스트 로그는 `local-notes/ai-work-log/`와 `local-notes/career/`를 따른다.

---

## 현재 상태 요약

| 항목 | 값 |
| --- | --- |
| 마지막 갱신 | 2026-06-26 |
| 현재 active milestone | `M5.5` Dispatch fencing hardening 구현 완료, 최종 정리 단계 |
| M2 상태 | 완료된 기반으로 취급 |
| M3 상태 | 완료 |
| M4 상태 | 완료 |
| 다음 권장 작업 | M5.5 변경 atomic commit 후 M6 설계문서 확인 |
| 최신 완료 milestone 작업 로그 | `local-notes/ai-work-log/2026-06-17-M5.md` |
| 최신 진행 milestone 작업 로그 | `local-notes/ai-work-log/2026-06-26-M5.5.md` |
| 최신 전체 검증 | 2026-06-26 `./gradlew --gradle-user-home .gradle test --rerun-tasks` 성공 |

---

## Milestone별 진행 상태

| Milestone | 상태 | 근거 / 비고 |
| --- | --- | --- |
| `M0` 프로젝트 기반 | 완료된 기반으로 취급 | 현재 multi-module, service app, Gradle/Docker/Test 기반 위에서 M1/M2 작업 진행 중 |
| `M1` Order 도메인 코어 | 완료된 기반으로 취급 | 주문 생성/취소 skeleton, 상태 전이, DB/API 기반 위에서 M2 outbox 연동 완료 |
| `M2` Reliable messaging 기반 | 완료된 기반으로 취급 | outbox, publisher, processed guard, envelope, traceId, 최소 parking log 구현 및 리뷰 루프 완료 후 M3 착수 기준선으로 사용 |
| `M3` Broker protocol과 Simulator | 완료 | broker-protocol codec, malformed 분류, Broker Simulator TCP server/admin API, ACK/RJCT/OSTS/duplicate fill 검증 구현, 문서화와 커밋 완료 |
| `M4` 정상 주문 end-to-end | 완료 | SubmitOrderCommand -> Gateway ORDR -> ACKN/RJCT/FILL canonical broker event -> Order Service 상태 반영 -> 최소 SSE까지 구현. 구현 커밋 `dddb734`, 설계 정합성 보정 커밋 `03a5f9d`, completion docs/tag workflow로 종료 |
| `M5` 기본 취소 흐름 | 완료 | `LIVE`/`PENDING_ACK` 주문 취소 요청, Gateway `CancelOrderCommand` durable attempt, `CXLQ`, `CXLA`/`CXLR`, Order Service cancel ACK/Reject 반영 구현. V1-V3 검증 루프와 full Gradle test 완료 |
| `M5.5` Dispatch fencing hardening | 구현 완료, 최종 정리 단계 | M6 착수 전 특별 편성. Gateway command attempt에 dispatch token/owner/lock을 도입하고 `ack_deadline_at`을 broker 응답 deadline 의미로 축소. V1-V3+closure 검증 루프와 full Gradle test 성공, commit/tag는 아직 수행하지 않음 |
| `M6` 부분체결 후 취소와 경합 | 착수 대기 | M5 완료 기준선 위에서 부분체결 후 취소, 취소 중 추가 체결, 수량 기준 cancel/fill race matrix 설계 확인 필요 |

상태 표현 기준:

* `미착수`: 설계만 있고 구현 thread가 시작되지 않음
* `진행 중`: 구현 또는 리뷰 피드백 반영 중
* `구현 완료, 최종 정리 단계`: 주요 구현과 검증은 끝났으나 커밋/PR/문서 동기화 확인이 남음
* `완료`: 커밋/PR 또는 사용자 승인 기준으로 milestone 종료 처리됨
* `착수 대기`: 이전 milestone 완료 후 설계 확인과 구현 계획 수립이 필요한 상태

---

## M5.5 구현/검증 메모

M5.5 범위는 M5 완료 후 특별 편성한 Broker Gateway dispatch fencing hardening이다.

현재 구현은 다음 기준을 만족하는 상태로 판단한다.

* `broker_command_attempt`에 `dispatch_token`, `dispatch_owner`, `dispatch_locked_until`을 추가하고, 기존 `CREATED + ack_deadline_at` row는 migration에서 dispatch lock으로 backfill한다.
* `ack_deadline_at`은 `SENT` 이후 broker response deadline으로만 사용한다.
* submit/cancel claim은 token/owner/lock을 발급하고, lock 만료 또는 OUT journal 존재 여부를 함께 확인한다.
* OUT journal insert는 현재 token과 lock이 유효할 때만 성공한다.
* `broker_message_journal`은 OUT 전용 generated column unique 제약으로 같은 broker/msg/wire OUT journal 중복 저장을 거절한다.
* TCP send 직전에도 token과 lock을 재확인한다.
* OUT journal 이후 TCP send 실패는 attempt를 `FAILED`로 단정하지 않고 직접 재송신도 하지 않는다.
* OUT `ORDR`/`CXLQ` journal 이후 불확실한 `CREATED` attempt와 응답 deadline이 만료된 `SENT` attempt는 Gateway 내부 `UNKNOWN`/parking으로 격리한다.
* unclaimed `CREATED` submit ACKN/RJCT는 binding/outbox 없이 parking된다.
* M5.5는 Broker Gateway 내부 DB/config-only 변경이며, public HTTP API, Kafka event contract, broker wire protocol은 변경하지 않는다.

검증:

* 2026-06-25 `git diff --check`
* 2026-06-25 `./gradlew --gradle-user-home .gradle :apps:broker-gateway-service:test --tests '*BrokerCommandServiceIntegrationTest' --tests '*BrokerGatewayCommandDispatchIntegrationTest' --tests '*BrokerGatewayInboundServiceIntegrationTest'`
* 2026-06-25 `./gradlew --gradle-user-home .gradle test --rerun-tasks`
* 2026-06-25 `./gradlew --gradle-user-home .gradle :apps:broker-gateway-service:test --tests com.trading.orderreliability.gateway.messaging.command.BrokerCommandServiceIntegrationTest`
* 2026-06-26 `./gradlew --gradle-user-home .gradle :apps:broker-gateway-service:test --tests com.trading.orderreliability.gateway.application.BrokerGatewayCommandDispatchIntegrationTest`
* 2026-06-26 `./gradlew --gradle-user-home .gradle :apps:broker-gateway-service:test --tests com.trading.orderreliability.gateway.messaging.command.BrokerCommandServiceIntegrationTest --tests com.trading.orderreliability.gateway.application.BrokerGatewayCommandDispatchIntegrationTest`
* 2026-06-26 `git diff --check`
* 2026-06-26 `./gradlew --gradle-user-home .gradle test --rerun-tasks`

M5.5에서 의도적으로 아직 하지 않는 것:

* canonical submit UNKNOWN event 발행과 Order Service `UNKNOWN` 상태 수렴. 이는 M7 범위다.
* cancel response timeout의 canonical event 발행, `PENDING_CANCEL -> UNKNOWN`, cancel reconciliation. 이는 M8 범위다.
* Flyway를 V2까지만 적용한 뒤 V3만 replay하는 별도 migration test. 현재는 legacy row backfill SQL 의미를 focused integration test로 검증한다.

---

## M3 완료 판정 메모

M3 범위는 `docs/13-development-milestones.md`의 "Broker protocol과 Simulator"이다.
현재 구현은 다음 기준을 만족하는 상태로 판단한다.

* `libs:broker-protocol`이 10-A fixed-length TCP protocol의 8 byte length header, 192 byte common header, msgId별 body encode/decode를 제공한다.
* `ORDR`, `ACKN`, `RJCT`, `FILL`, `CXLQ`, `CXLA`, `CXLR`, `EXPR`, `OSTQ`, `OSTS` round-trip이 codec test로 고정됐다.
* malformed frame/header/body와 business anomaly가 분리된다.
* Broker Simulator가 Netty TCP server로 `ORDR -> ACKN/RJCT`, `OSTQ -> OSTS`를 처리한다.
* local/test profile 전용 admin API가 scenario 전환, reset, 주문 조회, duplicate fill injection을 제공한다.
* duplicate fill injection은 동일 논리 `FILL` 2회 전송과 동일 `wireMessageId`, 원 주문 `traceId` 보존을 검증한다.
* Broker Simulator 주요 TCP 흐름은 `docs/14-api-sequence-diagrams.md`에 sequence diagram으로 기록됐다.
* 최종 검증 `./gradlew --gradle-user-home .gradle test`가 성공했다.

M3에서 의도적으로 아직 하지 않는 것:

* Broker Gateway TCP client와 Kafka command consumer 연결
* Gateway DB journal, command attempt, broker order binding 완성
* canonical broker event 발행
* Order Service broker event consumer와 상태 반영 end-to-end
* timeout, cancel, reconciliation, partial fill/cancel race, 전체 상태조회 matrix

위 항목은 M4 이후, 특히 M4/M5/M7/M8/M9/M10에서 다룬다.

---

## M4 완료 판정 메모

M4 범위는 `docs/13-development-milestones.md`의 "정상 주문 End-to-End"이다.
현재 구현은 다음 기준을 만족하는 상태로 판단한다.

* `libs:common-messaging`에 broker event payload와 message type 상수를 추가했다.
* Broker Gateway가 `SubmitOrderCommand`를 processed message로 멱등 처리하고, durable command attempt를 저장한 뒤 transaction 밖 dispatcher에서 `ORDR`를 송신한다.
* Gateway inbound handler가 수신 frame을 journal에 기록하고, `ACKN/RJCT/FILL`을 binding/attempt 갱신과 broker event outbox insert로 변환한다.
* M4 완료 시점에는 Gateway가 범위 밖 `CancelOrderCommand`, `QueryOrderStatusCommand`를 TCP 송신하지 않고 `UNSUPPORTED_COMMAND`로 parking했다. M5 개발에서 `CancelOrderCommand`는 durable `CANCEL` attempt와 `CXLQ` 송신 흐름으로 확장됐고, `QueryOrderStatusCommand`는 후속 milestone 범위로 남아 있다.
* M4 완료 시점의 Gateway command dispatch는 설계된 `transport_state` 값만 사용했고, `CREATED + ack_deadline_at IS NULL`을 dispatch claim 대상으로 삼았다. 이 worker lease와 broker response deadline 혼합은 M5.5에서 `dispatch_token`/`dispatch_owner`/`dispatch_locked_until`과 `ack_deadline_at`으로 분리됐다.
* M4 완료 시점에는 `CREATED/SENT + ack_deadline_at` 만료 attempt를 재전송하지 않고 내부 상태 `UNKNOWN` 및 `SUBMIT_OUTCOME_UNKNOWN` parking으로 격리했다. M5.5 이후 `CREATED` lease 만료와 `SENT` response deadline은 별도 컬럼으로 판단한다. `UNKNOWN` canonical event 발행은 M7 범위로 남긴다.
* Order Service가 `trading.broker.event.v1` consumer를 통해 ACK/Reject/Partial Fill/Full Fill을 processed message, dedup key, order event와 함께 적용한다.
* Order Service는 동일 dedup key + 동일 payload hash를 skip하고, 동일 key + 다른 payload hash를 상태 변경 없이 parking한다.
* 최소 SSE endpoint `GET /api/orders/stream`이 `order-status-changed` 이벤트를 발행한다.
* Broker Simulator는 `POST /api/simulator/orders/{orderId}/fills`로 partial/full fill 주입을 지원한다.
* M4 E2E smoke는 HTTP 주문 생성, Order outbox publish, Gateway Kafka consume, TCP `ORDR`, fake broker `ACKN`, Gateway broker event outbox publish, Order Service `LIVE` 조회까지 검증한다.
* RJCT/FILL은 Gateway inbound, Order Service broker event application, Simulator fill API focused/integration tests로 검증했다.

검증:

* 2026-06-16 `git diff --check`
* 2026-06-16 `./gradlew --gradle-user-home .gradle :apps:broker-gateway-service:test --tests '*SubmitOrderEndToEndSmokeIntegrationTest' --tests '*BrokerGatewayCommandDispatchIntegrationTest' --tests '*BrokerGatewayInboundServiceIntegrationTest' --tests '*BrokerCommandServiceIntegrationTest' :apps:order-service:test --tests '*BrokerEventApplicationServiceIntegrationTest' --tests '*BrokerEventConsumerIntegrationTest' --tests '*OrderStatusSsePublisherIntegrationTest' :apps:broker-simulator:test --tests '*BrokerSimulatorTcpIntegrationTest'`
* 2026-06-16 `./gradlew --gradle-user-home .gradle test`

M4에서 의도적으로 아직 하지 않는 것:

* cancel 정상/거절 흐름과 partial fill 이후 cancel race
* submit timeout의 canonical `UNKNOWN` event 발행과 reconciliation
* stale/EOD sweep, replay/reinject 도구, 운영 metric/dashboard
* 계정별 SSE 필터링과 production-grade SSE fanout
* 실제 Broker Simulator admin fill API까지 연결한 partial/full fill broad E2E smoke 확장. 기능 자체는 Simulator/Gateway/Order focused tests로 검증했고, 후속 회귀/경합 시나리오에서 회수한다.

---

## M5 완료 판정 메모

M5 범위는 `docs/13-development-milestones.md`의 "기본 취소 흐름"이다.
현재 구현은 다음 기준을 만족하는 상태로 판단한다.

* Order Service가 `LIVE`와 `PENDING_ACK` 주문 취소 요청을 `CANCEL` instruction과 `PENDING_CANCEL` 상태로 보존하고 `CancelOrderCommand` outbox를 발행한다.
* Gateway가 `CancelOrderCommand`를 processed message로 멱등 처리하고 durable `CANCEL` command attempt를 저장한다.
* Gateway는 accepted `broker_order_binding`과 `brokerOrderId`가 확보된 cancel attempt만 `CXLQ` dispatch 후보로 본다.
* Gateway dispatcher는 `CXLQ` OUT journal과 TCP 송신을 수행하고, `CXLA`/`CXLR` inbound frame을 canonical `BrokerCancelAcknowledged`/`BrokerCancelRejected` outbox event로 변환한다.
* duplicate `CXLA`/`CXLR`, unknown `wireMessageId`, `brokerOrderId` mismatch, unclaimed cancel response는 order 상태를 오염시키지 않고 journal-only 또는 parking 경로로 격리한다.
* `PENDING_ACK` 취소 이후 submit `ACKN`은 주문을 `PENDING_CANCEL`로 유지하고 PLACE instruction만 완료한다.
* `PENDING_ACK` 취소 이후 submit `RJCT`는 주문을 `REJECTED`로 닫고 active `CANCEL` instruction을 `NOT_APPLIED`로 정리하며, 미송신 Gateway `CANCEL` attempt를 `FAILED`로 종결한다.
* `CXLA` 수신 후 Order Service는 주문을 `CANCELED`로 종결하고 active `CANCEL` instruction을 `COMPLETED`로 처리한다.
* `CXLR` 수신 후 Order Service는 체결 수량 기준의 유효 상태로 수렴하고 active `CANCEL` instruction을 `REJECTED`로 처리한다.
* `FILLED` terminal 이후 늦은 cancel ACK/Reject는 주문 상태를 되돌리지 않고 active `CANCEL` instruction을 `NOT_APPLIED`로 정리한다.
* Broker Simulator가 `CANCEL_ACK_SUCCESS`/`CANCEL_REJECT_SUCCESS` 시나리오에서 `CXLQ`에 대해 `CXLA`/`CXLR`를 반환한다.
* M5 검증 루프는 V1-V3까지 수행했고, BLOCKER/MAJOR/중요 TEST GAP이 없는 상태로 종료했다.

검증:

* 2026-06-17 `git diff --check`
* 2026-06-17 `./gradlew --gradle-user-home .gradle :apps:order-service:test --tests '*OrderStateMachineContractTest' --tests '*BrokerEventApplicationServiceIntegrationTest' :apps:broker-gateway-service:test --tests '*BrokerCommandServiceIntegrationTest' --tests '*BrokerGatewayInboundServiceIntegrationTest' --tests '*BrokerGatewayCommandDispatchIntegrationTest' :apps:broker-simulator:test --tests '*BrokerSimulatorTcpIntegrationTest'`
* 2026-06-25 `git diff --check`
* 2026-06-25 `./gradlew --gradle-user-home .gradle test --rerun-tasks`

M5에서 의도적으로 아직 하지 않는 것:

* 부분체결 후 취소와 취소 중 추가 fill 수량 race matrix
* `CXLQ` response timeout 이후 canonical event 발행, `PENDING_CANCEL -> UNKNOWN`
* cancel outcome unknown 기반 reconciliation과 상태조회 snapshot 수렴
* M5 완료 시점에는 OUT journal unique 제약과 dispatch token/owner/lock 기반 다중 dispatcher hardening이 남아 있었다. 두 항목 모두 M5.5에서 회수했다.
* Order Service Kafka consumer 경로의 cancel event 직접 테스트, `CXLR` mismatch/unknown wire 대칭 negative test

---

## M2 완료 판정 메모

M2 범위는 `docs/13-development-milestones.md`의 "Reliable messaging 기반"이다.
현재 구현은 다음 기준을 만족하는 상태로 판단한다.

* 주문 생성과 같은 DB transaction에서 `SubmitOrderCommand` outbox 저장
* outbox publisher가 Kafka 발행 성공 후 `SENT` 갱신
* publish 성공 후 `SENT` 갱신 실패를 중복 발행 가능성으로 모델링
* processed message guard로 같은 `consumerName + messageId` business handler 중복 실행 방지
* API request `traceId`를 outbox header와 Kafka envelope까지 전파
* service별 `outbox_message`, `processed_message`, `parked_message` migration 준비
* consumer 실패와 parking의 최소 정책을 DB parking log와 `MessageParkingLot` 테스트로 고정

M2에서 의도적으로 아직 하지 않는 것:

* Gateway TCP 송신
* Broker Simulator 실제 응답
* Recovery job orchestration
* 실제 Kafka consumer flow에서 반복 실패 5회 후 parking 처리
* parking message replay/reinject 도구
* 운영 콘솔/UI

위 항목은 M3 이후, 특히 M4/M5/M10/M11에서 다룬다.

---

## M2 주요 산출물

코드:

* `libs/common-messaging/src/main/java/com/trading/orderreliability/common/messaging/MessageEnvelope.java`
* `libs/common-messaging/src/main/java/com/trading/orderreliability/common/messaging/MessagingTopics.java`
* `libs/common-messaging/src/main/java/com/trading/orderreliability/common/messaging/MessageTypes.java`
* `libs/common-messaging/src/main/java/com/trading/orderreliability/common/messaging/BrokerCommandPayloads.java`
* `apps/order-service/src/main/java/com/trading/orderreliability/order/adapter/out/messaging/`
* `apps/order-service/src/main/resources/db/migration/V2__create_order_messaging_tables.sql`
* `apps/broker-gateway-service/src/main/resources/db/migration/V1__create_gateway_messaging_tables.sql`
* `apps/recovery-service/src/main/resources/db/migration/V1__create_recovery_messaging_tables.sql`

테스트:

* `OrderOutboxIntegrationTest`
* `OutboxPublisherIntegrationTest`
* `ProcessedMessageGuardIntegrationTest`
* `MessageParkingLotIntegrationTest`

문서:

* `docs/09-database-design.md`
* `docs/10-api-event-protocol-spec.md`
* `docs/11-failure-and-retry-policy.md`
* `docs/12-test-monitoring-operations.md`
* `docs/13-development-milestones.md`
* `local-notes/ai-work-log/2026-06-10-M2.md`

---

## M2에서 다음 thread가 반드시 확인할 위험

1. `headers_json NOT NULL`은 현재 구현 정책과 맞지만, DB 설계 표기와 차이가 남아 있는지 확인한다.
2. `CancelOrderCommand` outbox 저장은 M2에서 선행 구현했지만, Gateway TCP 송신/취소 응답 처리는 M5 범위다.
3. `OrderPrice`는 trailing zero를 제거한다. broker command price scale 원문 보존이 필요한지 M4/M5 전에 재검토한다.
4. parking log는 M2에서 격리 저장소까지만 제공한다. 실제 consumer offset commit, 반복 실패 threshold, replay tooling은 후속 milestone에서 연결한다.
5. Embedded Kafka shutdown 중 임시 snapshot 관련 warning/error log가 나올 수 있으나, 마지막 전체 Gradle test 결과는 성공이었다.

---

## 새 thread 시작 시 권장 행동

개발 요청이 milestone을 명시하지 않으면 다음처럼 판단한다.

1. 요청이 정상 주문 end-to-end, Broker Gateway command consumer, Gateway TCP client, canonical broker event, Order Service broker event consumer 연결이라면 M4 완료 상태를 먼저 확인하고 regression/follow-up인지 판단한다.
2. 요청이 M3 broker protocol/TCP frame/Simulator 보강이면 M3 완료 상태와 호환성 영향을 먼저 확인한다.
3. 요청이 취소 command 흐름이면 `M5`, 부분체결 후 취소 경합이면 `M6`, submit uncertainty/reconciliation이면 `M7`, cancel uncertainty/reconciliation이면 `M8` 범위로 판단한다.
4. 요청이 parking consumer handler 완성, 반복 실패 5회 후 parking, malformed/stale/EOD hardening이면 `M10` 범위일 수 있으므로 M4 정상 주문 E2E 작업으로 끌어오지 않는다.

작업 후에는 이 문서의 "현재 상태 요약", "Milestone별 진행 상태", "다음 thread가 반드시 확인할 위험"을 갱신한다.
