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
| 마지막 갱신 | 2026-06-13 |
| 현재 active milestone | `M4` 정상 주문 end-to-end 구현 완료, 최종 정리 단계 |
| M2 상태 | 완료된 기반으로 취급 |
| M3 상태 | 완료 |
| 다음 권장 작업 | M4 최종 리뷰/커밋 또는 milestone completion workflow 진행 |
| 최신 완료 milestone 작업 로그 | `local-notes/ai-work-log/2026-06-13-M4.md` |
| 최신 전체 검증 | `./gradlew --gradle-user-home .gradle test` 성공 |

---

## Milestone별 진행 상태

| Milestone | 상태 | 근거 / 비고 |
| --- | --- | --- |
| `M0` 프로젝트 기반 | 완료된 기반으로 취급 | 현재 multi-module, service app, Gradle/Docker/Test 기반 위에서 M1/M2 작업 진행 중 |
| `M1` Order 도메인 코어 | 완료된 기반으로 취급 | 주문 생성/취소 skeleton, 상태 전이, DB/API 기반 위에서 M2 outbox 연동 완료 |
| `M2` Reliable messaging 기반 | 완료된 기반으로 취급 | outbox, publisher, processed guard, envelope, traceId, 최소 parking log 구현 및 리뷰 루프 완료 후 M3 착수 기준선으로 사용 |
| `M3` Broker protocol과 Simulator | 완료 | broker-protocol codec, malformed 분류, Broker Simulator TCP server/admin API, ACK/RJCT/OSTS/duplicate fill 검증 구현, 문서화와 커밋 완료 |
| `M4` 정상 주문 end-to-end | 구현 완료, 최종 정리 단계 | SubmitOrderCommand -> Gateway ORDR -> ACKN/RJCT/FILL canonical broker event -> Order Service 상태 반영 -> 최소 SSE까지 구현. 완료 처리는 커밋/태그 workflow 전이라 아직 `완료`로 표시하지 않음 |

상태 표현 기준:

* `미착수`: 설계만 있고 구현 thread가 시작되지 않음
* `진행 중`: 구현 또는 리뷰 피드백 반영 중
* `구현 완료, 최종 정리 단계`: 주요 구현과 검증은 끝났으나 커밋/PR/문서 동기화 확인이 남음
* `완료`: 커밋/PR 또는 사용자 승인 기준으로 milestone 종료 처리됨
* `착수 대기`: 이전 milestone 완료 후 설계 확인과 구현 계획 수립이 필요한 상태

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

## M4 구현 완료, 최종 정리 단계 판정 메모

M4 범위는 `docs/13-development-milestones.md`의 "정상 주문 End-to-End"이다.
현재 구현은 다음 기준을 만족하는 상태로 판단한다.

* `libs:common-messaging`에 broker event payload와 message type 상수를 추가했다.
* Broker Gateway가 `SubmitOrderCommand`를 processed message로 멱등 처리하고, durable command attempt를 저장한 뒤 transaction 밖 dispatcher에서 `ORDR`를 송신한다.
* Gateway inbound handler가 수신 frame을 journal에 기록하고, `ACKN/RJCT/FILL`을 binding/attempt 갱신과 broker event outbox insert로 변환한다.
* Gateway는 M4 범위 밖 `CancelOrderCommand`, `QueryOrderStatusCommand`를 TCP 송신하지 않고 `UNSUPPORTED_COMMAND_FOR_M4`로 parking한다.
* stale `DISPATCHING` attempt는 재전송하지 않고 내부 상태 `UNKNOWN` 및 parking으로 격리한다. `UNKNOWN` canonical event 발행은 M7 범위로 남긴다.
* Order Service가 `trading.broker.event.v1` consumer를 통해 ACK/Reject/Partial Fill/Full Fill을 processed message, dedup key, order event와 함께 적용한다.
* Order Service는 동일 dedup key + 동일 payload hash를 skip하고, 동일 key + 다른 payload hash를 상태 변경 없이 parking한다.
* 최소 SSE endpoint `GET /api/orders/stream`이 `order-status-changed` 이벤트를 발행한다.
* Broker Simulator는 `POST /api/simulator/orders/{orderId}/fills`로 partial/full fill 주입을 지원한다.
* M4 E2E smoke는 HTTP 주문 생성, Order outbox publish, Gateway Kafka consume, TCP `ORDR`, fake broker `ACKN`, Gateway broker event outbox publish, Order Service `LIVE` 조회까지 검증한다.

검증:

* `git diff --check`
* `./gradlew --gradle-user-home .gradle :apps:broker-gateway-service:test --tests '*SubmitOrderEndToEndSmokeIntegrationTest'`
* `./gradlew --gradle-user-home .gradle :libs:common-messaging:test :apps:broker-simulator:test :apps:broker-gateway-service:test :apps:order-service:test`
* `./gradlew --gradle-user-home .gradle test`

M4에서 의도적으로 아직 하지 않는 것:

* cancel 정상/거절 흐름과 partial fill 이후 cancel race
* submit timeout의 canonical `UNKNOWN` event 발행과 reconciliation
* stale/EOD sweep, replay/reinject 도구, 운영 metric/dashboard
* 계정별 SSE 필터링과 production-grade SSE fanout

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

1. 요청이 정상 주문 end-to-end, Broker Gateway command consumer, Gateway TCP client, canonical broker event, Order Service broker event consumer 연결이라면 `M4`로 판단한다.
2. 요청이 M3 broker protocol/TCP frame/Simulator 보강이면 M4 착수 전에 M3 완료 상태와 호환성 영향을 먼저 확인한다.
3. 요청이 취소 command 흐름이면 `M5`, 부분체결 후 취소 경합이면 `M6`, submit uncertainty/reconciliation이면 `M7`, cancel uncertainty/reconciliation이면 `M8` 범위로 판단한다.
4. 요청이 parking consumer handler 완성, 반복 실패 5회 후 parking, malformed/stale/EOD hardening이면 `M10` 범위일 수 있으므로 M4 정상 주문 E2E 작업으로 끌어오지 않는다.

작업 후에는 이 문서의 "현재 상태 요약", "Milestone별 진행 상태", "다음 thread가 반드시 확인할 위험"을 갱신한다.
