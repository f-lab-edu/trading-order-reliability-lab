# 12. 테스트 / 모니터링 / 운영 계획

## 12.1 목적

이 문서는 Trading Order Reliability Lab의 테스트 전략, 모니터링 지표, 운영 절차를 정의한다.

앞선 문서들이 주문 상태 모델, 메시징, DB, API, 장애 처리 정책을 정의했다면, 이 문서는 다음 질문에 답한다.

1. 어떤 테스트로 주문 신뢰성 요구사항을 검증할 것인가?
2. 장애, 중복, 순서 역전, malformed, reconciliation 시나리오를 어떻게 재현할 것인가?
3. 어떤 지표와 로그로 주문 처리 흐름을 추적할 것인가?
4. Outbox, consumer, Gateway, Recovery 실패는 어떤 기준으로 운영 경보를 발생시킬 것인가?
5. 11장에서 미룬 재시도 횟수, timeout, parking 정책의 초기값은 무엇인가?
6. 운영자가 특정 주문 장애를 어떤 순서로 조사할 것인가?
7. Phase 1에서 구현할 운영 기능과 후속 단계로 미룰 기능은 무엇인가?

---

## 12.2 기본 원칙

### 12.2.1 테스트는 상태 수렴을 검증해야 한다

이 프로젝트의 핵심은 단일 API 성공 여부가 아니라 **주문이 최종적으로 올바른 상태로 수렴하는지**다.

따라서 테스트는 다음을 확인해야 한다.

* 정상 주문은 `PENDING_ACK` 이후 `LIVE` 또는 terminal 상태로 수렴한다.
* 외부 command 결과가 불확실하면 임의 실패가 아니라 `UNKNOWN + reconciliation`으로 전환된다.
* 중복 메시지와 중복 브로커 이벤트는 상태를 중복 변경하지 않는다.
* 순서 역전 이벤트는 상태머신 규칙 안에서 유효한 상태로 수렴한다.
* malformed 전문은 주문 상태를 직접 오염시키지 않는다.
* stale 또는 EOD non-terminal 주문은 방치되지 않고 reconciliation 대상으로 식별된다.

---

### 12.2.2 운영 관측은 도메인 식별자를 중심으로 한다

운영자는 다음 식별자로 흐름을 추적할 수 있어야 한다.

| 식별자 | 주요 용도 |
| --- | --- |
| `orderId` | 주문 생애주기 추적의 중심 |
| `clientOrderId` | 사용자 주문 생성 멱등성 확인 |
| `clientCancelRequestId` | 사용자 취소 요청 멱등성 확인 |
| `instructionId` | `PLACE`, `CANCEL` instruction 처리 추적 |
| `messageId` | 내부 메시지 envelope 추적 |
| `brokerEventDedupKey` | 같은 외부 사건의 중복 반영 방지 확인 |
| `wireMessageId` | 브로커 TCP 전문 단위 correlation |
| `traceId` | API, 메시지, Gateway, Recovery 흐름 연결 |
| `jobId` | reconciliation job 추적 |
| `attemptId` | reconciliation 상태조회 attempt 추적 |

운영 지표와 로그는 이 식별자들을 최대한 구조화된 필드로 포함한다.

---

### 12.2.3 메트릭은 "문제 발견", 이력은 "원인 분석"에 사용한다

메트릭은 이상 징후를 빠르게 발견하기 위한 수단이다.

예:

* `UNKNOWN` 진입 수 증가
* malformed 전문 증가
* outbox backlog 증가
* reconciliation 실패율 증가
* Gateway command timeout 증가

반면 원인 분석은 DB 이력과 구조화 로그를 기준으로 한다.

예:

* `trade_order`
* `order_instruction`
* `order_event`
* `broker_command_attempt`
* `broker_message_journal`
* `reconciliation_job`
* `reconciliation_attempt`
* `outbox_message`
* `processed_message`

---

### 12.2.4 Phase 1 운영은 자동 복구보다 추적 가능성을 우선한다

Phase 1은 개인 프로젝트의 신뢰성 실험 환경이다.

따라서 운영 자동화는 과하게 만들지 않는다.

| 항목 | Phase 1 기준 |
| --- | --- |
| 자동 재시도 | Outbox, consumer, reconciliation 상태조회에 한정 |
| 자동 주문 종결 | `NOT_FOUND` 등 애매한 결과로 자동 종결하지 않음 |
| 운영 콘솔 | 별도 UI보다 조회 API, DB 이력, 로그, dashboard 우선 |
| 수동 개입 | 수동 reconciliation trigger와 job abort 정도만 고려 |
| 알림 | 로컬/개발 환경에서는 dashboard와 log alert 중심 |

---

## 12.3 테스트 범위

## 12.3.1 테스트 레벨

| 레벨 | 대상 | 목적 |
| --- | --- | --- |
| Unit Test | 상태머신, validator, idempotency policy, retry policy | 도메인 규칙을 빠르게 검증 |
| Parser/Serializer Test | Gateway TCP frame, fixed-length body | 전문 파싱/생성 오류 방지 |
| Contract Test | API schema, event schema, broker command/event schema | 서비스 간 계약 변경 감지 |
| Repository Test | DB constraint, index, lock, unique key | 멱등성과 중복 방지 보장 |
| Component Test | 단일 서비스 + DB + fake message adapter | 서비스 내부 transaction boundary 검증 |
| Integration Test | Order, Gateway, Recovery, Kafka, MySQL, Broker Simulator | 실제 흐름과 장애 수렴 검증 |
| End-to-End Test | 사용자 API부터 브로커 이벤트, SSE, 조회 API까지 | 사용자 관점의 결과 확인 |
| Reliability Scenario Test | 장애 주입, 중복, 순서 역전, malformed, timeout | 품질 속성 시나리오 검증 |
| Performance Smoke Test | 주요 latency와 backlog | 회귀 탐지용 초기 벤치마크 |
| Operational Drill | 운영 조사 runbook, dashboard, 수동 trigger | 장애 추적 가능성 검증 |

---

## 12.3.2 테스트 피라미드 기준

Phase 1에서는 느린 E2E 테스트보다 도메인 상태머신과 integration scenario test에 집중한다.

| 테스트 유형 | 비중 | 기준 |
| --- | ---: | --- |
| Unit / Parser / Policy | 높음 | 빠르게 자주 실행 |
| Contract / Repository | 중간 | 스키마와 DB 제약 변경 시 필수 |
| Integration Scenario | 높음 | 신뢰성 요구사항의 핵심 증명 |
| E2E / SSE | 중간 | 사용자-facing 흐름만 선별 |
| Performance | 낮음 | 회귀 감지용 smoke 수준 |
| Long-running soak | 낮음 | Phase 1 후반 또는 Phase 2 |

---

## 12.4 테스트 환경 구성

## 12.4.1 로컬 통합 테스트 환경

로컬 통합 테스트는 다음 구성요소를 사용한다.

```text
Test Runner
  -> Order Service
  -> Broker Gateway Service
  -> Recovery Service
  -> Kafka
  -> MySQL
  -> External Broker Simulator
```

구성 원칙:

* Kafka topic은 테스트 시작 시 생성하고 종료 시 정리한다.
* MySQL schema는 migration으로 생성하고 테스트마다 격리한다.
* Broker Simulator는 시나리오 주입 API를 제공한다.
* 테스트 데이터는 `accountId`, `clientOrderId`, `traceId`에 test run prefix를 붙인다.
* 시간 의존 테스트는 가능한 한 clock abstraction 또는 짧은 test profile 값을 사용한다.

---

## 12.4.2 테스트 profile

운영 초기값과 테스트 실행값은 분리한다.

| 설정 | 운영/개발 기본값 | 테스트 profile 예 |
| --- | ---: | ---: |
| submit/cancel ACK deadline | 5초 | 300ms |
| query status deadline | 5초 | 300ms |
| stale sweep interval | 30초 | 500ms |
| `PENDING_ACK` stale threshold | 30초 | 1초 |
| `PENDING_CANCEL` stale threshold | 30초 | 1초 |
| `LIVE` stale threshold | 5분 | 2초 |
| `PARTIALLY_FILLED` stale threshold | 5분 | 2초 |
| reconciliation attempt max | 4회 | 2회 |
| outbox publisher interval | 1초 | 100ms |
| SSE assertion timeout | 3초 | 3초 |

테스트 profile은 장애 시나리오를 빠르게 재현하기 위한 값이며, 운영 기준을 의미하지 않는다.

---

## 12.4.3 Broker Simulator 시나리오 주입

Broker Simulator는 테스트 가능성을 위한 핵심 컴포넌트다.

Phase 1에서는 다음 시나리오를 주입할 수 있어야 한다.

| 시나리오 | 설명 | 검증 대상 |
| --- | --- | --- |
| `ACK_SUCCESS` | 주문 요청에 정상 ACK 반환 | 정상 주문 접수 |
| `REJECT_SUCCESS` | 주문 요청에 명시적 Reject 반환 | 주문 거절 반영 |
| `ACK_TIMEOUT` | 주문 요청 후 응답 없음 | `UNKNOWN` 전환 |
| `CANCEL_ACK_SUCCESS` | 취소 요청에 정상 CancelAck 반환 | 취소 완료 |
| `CANCEL_REJECT_SUCCESS` | 취소 요청에 CancelReject 반환 | 취소 거절 처리 |
| `CANCEL_TIMEOUT` | 취소 요청 후 응답 없음 | cancel outcome unknown |
| `FILL_BEFORE_ACK` | ACK보다 체결 이벤트 먼저 전송 | 순서 역전 수렴 |
| `DUPLICATE_FILL` | 같은 체결 이벤트 중복 전송 | dedup 검증 |
| `PARTIAL_THEN_CANCEL_RACE` | 부분체결과 취소 응답 경합 | 수량 불변식 |
| `MALFORMED_FRAME` | frame 길이 또는 구조 오류 | Gateway 격리 |
| `MALFORMED_HEADER` | header 파싱 오류 | Gateway 격리 |
| `MALFORMED_BODY` | body 파싱 오류 | command outcome unknown 또는 격리 |
| `STATUS_ACCEPTED` | 상태조회 snapshot `ACCEPTED` | reconciliation 수렴 |
| `STATUS_PARTIAL` | 상태조회 snapshot `PARTIAL` | 부분체결 수렴 |
| `STATUS_FILLED` | 상태조회 snapshot `FILLED` | terminal 수렴 |
| `STATUS_CANCELED` | 상태조회 snapshot `CANCELED` | terminal 수렴 |
| `STATUS_REJECTED` | 상태조회 snapshot `REJECTED` | terminal 수렴 |
| `STATUS_EXPIRED` | 상태조회 snapshot `EXPIRED` | terminal 수렴 |
| `STATUS_NOT_FOUND` | 상태조회 snapshot `NOT_FOUND` | 자동 종결 금지 |
| `STATUS_TIMEOUT` | 상태조회 응답 없음 | attempt retry |

시나리오 주입 API는 로컬/테스트 환경 전용으로 둔다.

M3의 최소 Simulator admin API는 TCP protocol과 Simulator 완료 기준 검증에 필요한 좁은 범위로 둔다.
Phase 1 전체 시나리오 목록 중 timeout, cancel race, 순서 역전, 상태별 snapshot matrix는 M4 이후 Gateway/Order Service 연결 단계에서 확장한다.

| Method | Path | 목적 |
| --- | --- | --- |
| `PUT` | `/api/simulator/scenario` | `ACK_SUCCESS`, `REJECT_SUCCESS` 전환 |
| `POST` | `/api/simulator/reset` | in-memory 주문/이벤트 상태 초기화 |
| `GET` | `/api/simulator/orders` | Simulator 주문 상태 목록 조회 |
| `GET` | `/api/simulator/orders/{orderId}` | Simulator 주문 상태 단건 조회 |
| `POST` | `/api/simulator/orders/{orderId}/duplicate-fill` | 동일 논리 `FILL` 2회 전송과 동일 `wireMessageId` 재사용 검증 |

M3 Broker Simulator의 TCP request/response, malformed 격리, duplicate fill injection 흐름은 `docs/14-api-sequence-diagrams.md`의 "Broker Simulator M3 TCP 흐름" sequence diagram에 기록한다.

---

## 12.5 테스트 데이터 원칙

### 12.5.1 식별자 생성

테스트는 디버깅이 쉽도록 식별자에 의미 있는 prefix를 사용한다.

| 필드 | 예 |
| --- | --- |
| `accountId` | `ACC-TEST-001` |
| `clientOrderId` | `TC-ORDER-IDEMPOTENT-001` |
| `clientCancelRequestId` | `TC-CANCEL-RACE-001` |
| `traceId` | `trace-tc-unknown-submit-001` |
| `wireMessageId` | `W-TC-ACK-TIMEOUT-001` |

실제 DB PK인 UUID v7은 시스템 생성값을 사용한다.

---

### 12.5.2 테스트 격리

테스트 간 간섭을 막기 위해 다음 중 하나를 선택한다.

| 방식 | 설명 | Phase 1 권장 |
| --- | --- | --- |
| DB schema per test class | 테스트 클래스마다 schema 분리 | 선택 |
| Transaction rollback | 테스트 후 rollback | 단일 서비스 test에 적합 |
| Test run prefix | 데이터에 run id prefix 부여 후 정리 | 통합 테스트 권장 |
| Container 재생성 | 테스트 suite마다 DB/Kafka 재생성 | 느리지만 가장 명확 |

Phase 1 통합 테스트는 `testRunId` prefix + suite 종료 후 정리를 기본으로 한다.

---

## 12.6 핵심 테스트 매트릭스

## 12.6.1 요구사항 기반 매트릭스

| ID | 테스트 대상 | 주요 검증 |
| --- | --- | --- |
| `FR-001` | 주문 생성 | `trade_order`, `order_instruction`, outbox 생성 |
| `FR-002` | 주문 생성 멱등성 | 동일 payload 재요청은 기존 결과 반환, 다른 payload는 `409` |
| `FR-003` | 주문 조회 | account 경계, 현재 상태, instruction, reconciliation 상태 노출 |
| `FR-004` | 주문 취소 | 허용/거부 상태, active cancel 중복 방지 |
| `FR-005` | 상태 전이 | ACK, Reject, Fill, CancelAck, Expire 반영 |
| `FR-006` | 부분체결 | `cumQty`, `leavesQty`, 상태 불변식 |
| `FR-007` | 부분체결 후 취소 | 체결분 유지, 미체결 잔량 취소 |
| `FR-008` | DAY 주문 만료 | Expire 이벤트와 EOD reconciliation |
| `FR-009` | SSE | 상태 변경 알림, 재연결 후 조회 보완 |
| `FR-010` | 브로커 전문 통신 | frame, header, body, journal |
| `FR-011` | Broker Simulator | 장애 시나리오 주입 |
| `FR-012` | `UNKNOWN` | outcome unknown 시 격리 |
| `FR-013` | Reconciliation | job, attempt, snapshot, 결과 이벤트 |
| `FR-014` | 운영 추적 | 주문/전문/복구 이력 연결 |

---

## 12.6.2 품질 속성 기반 매트릭스

| ID | 테스트 시나리오 | 성공 기준 |
| --- | --- | --- |
| `QA-001` | 주문 접수 후 outbox publish 일시 실패 | 주문은 조회 가능하고 outbox 재시도로 command 발행 |
| `QA-002` | 취소 요청 후 cancel ACK timeout | cancel 의도 유지, reconciliation 후 필요한 경우 cancel 재발행 |
| `QA-003` | 같은 Fill 이벤트 중복 수신 | 수량은 한 번만 반영, 중복 이력 추적 |
| `QA-004` | Fill이 ACK보다 먼저 도착 | 주문은 유효한 filled/partial 상태로 수렴 |
| `QA-005` | submit/cancel 응답 timeout | 임의 실패 없이 `UNKNOWN + PENDING` |
| `QA-006` | 오래 머문 non-terminal | stale detector가 reconciliation 요청 |
| `QA-007` | 브로커 장애 중 주문 조회 | 조회 API는 마지막 저장 상태 반환 |
| `QA-008` | 특정 주문 장애 조사 | `orderId` 기준으로 상태, 전문, recovery 이력 연결 가능 |
| `QA-009` | 정상 이벤트 처리 latency | 초기 벤치마크 목표 이하 |
| `QA-010` | Gateway event contract 변경 | Order Service 도메인 모델 영향 최소화 |
| `QA-011` | 장애 시나리오 반복 실행 | 같은 입력으로 같은 수렴 결과 재현 |

---

## 12.7 상세 테스트 시나리오

## 12.7.1 주문 생성 정상 흐름

목적:

* 사용자 주문이 `PENDING_ACK`로 저장되고 브로커 ACK 후 `LIVE`로 수렴하는지 검증한다.

절차:

1. `POST /orders` 호출
2. `trade_order.status = PENDING_ACK` 확인
3. `order_instruction(type=PLACE, status=REQUESTED)` 확인
4. `SubmitOrderCommand` outbox 생성 확인
5. Broker Simulator가 `ACKN` 반환
6. Gateway가 `BrokerOrderAcknowledged` 발행
7. Order Service가 `LIVE`로 상태 전이
8. `PLACE` instruction 완료 확인
9. SSE 또는 조회 API에서 `LIVE` 확인

성공 기준:

* 주문이 중간에 사라지지 않는다.
* `order_event`에 생성과 상태 변경 이력이 남는다.
* `broker_command_attempt`와 `broker_message_journal`에서 송수신 전문이 추적된다.

---

## 12.7.2 주문 생성 멱등성

| 조건 | 기대 결과 |
| --- | --- |
| 같은 `accountId + clientOrderId` + 같은 payload | 기존 주문 결과 반환 |
| 같은 `accountId + clientOrderId` + 다른 수량 | `409 Conflict` |
| 같은 `clientOrderId`지만 다른 `accountId` | 별도 주문 허용 |
| 기존 주문이 terminal인 상태에서 같은 요청 재시도 | 기존 terminal 상태 반환 |

추가 검증:

* 같은 멱등 요청이 동시에 들어와도 주문 row는 하나만 생성된다.
* unique constraint 또는 row lock 정책이 race condition을 방어한다.

---

## 12.7.3 취소 요청 멱등성과 active cancel 중복

| 조건 | 기대 결과 |
| --- | --- |
| `LIVE` 주문 취소 | `CANCEL` instruction 생성, `PENDING_CANCEL` |
| 같은 `clientCancelRequestId` 재요청 | 기존 취소 결과 반환 |
| 다른 `clientCancelRequestId`로 active cancel 중복 요청 | `409 Conflict` |
| terminal 주문 취소 | 거절 |
| `UNKNOWN` 주문 취소 | 거절 |

추가 검증:

* 동시에 여러 취소 요청이 들어와도 active `CANCEL` instruction은 하나만 존재한다.
* 부분체결 후 취소는 `cumQty`를 변경하지 않고 잔량에 대한 취소로 처리된다.

---

## 12.7.4 브로커 이벤트 중복

시나리오:

1. 주문이 `LIVE` 상태가 된다.
2. Broker Simulator가 같은 논리 Fill 이벤트를 2회 전송한다.
3. Gateway는 동일하거나 동등한 `brokerEventDedupKey`를 부여한다.
4. Order Service는 첫 이벤트만 상태와 수량에 반영한다.
5. 두 번째 이벤트는 중복으로 기록하거나 무시한다.

성공 기준:

* `cumQty`와 `leavesQty`는 한 번만 변경된다.
* `order_event.dedup_key` 중복으로 상태 오염이 없다.
* duplicate metric이 증가한다.

---

## 12.7.5 메시지 envelope 중복 소비

시나리오:

1. 같은 `messageId`의 Kafka message를 consumer가 두 번 처리하게 만든다.
2. 첫 번째 처리는 비즈니스 transaction을 commit한다.
3. 두 번째 처리는 `processed_message`를 보고 business handler를 실행하지 않는다.

성공 기준:

* `processed_message(consumerName, messageId)`는 한 번만 기록된다.
* 주문 상태 변경은 한 번만 발생한다.
* consumer duplicate metric이 증가한다.

---

## 12.7.6 순서 역전 이벤트

| 시나리오 | 기대 결과 |
| --- | --- |
| ACK보다 partial fill 먼저 도착 | `PARTIALLY_FILLED`로 수렴 |
| ACK보다 full fill 먼저 도착 | `FILLED`로 수렴 |
| `FILLED` 후 늦은 ACK 도착 | terminal 상태 유지 |
| cancel 요청 중 Fill 먼저 도착 | 수량 기준으로 `FILLED` 또는 `PARTIALLY_FILLED/PENDING_CANCEL` 수렴 |
| `CANCELED` 후 늦은 partial fill 도착 | 상태머신이 유효성 판단, 불가능하면 anomaly 기록 |

성공 기준:

* 상태 전이는 수량 불변식을 위반하지 않는다.
* 늦은 이벤트가 terminal 상태를 오염시키지 않는다.
* anomaly 발생 시 주문 상태를 임의 변경하지 않고 추적 이력을 남긴다.

---

## 12.7.7 submit 결과 불확실

시나리오:

1. 주문 생성 후 Gateway가 `ORDR` 전문을 송신한다.
2. Broker Simulator가 ACK/Reject를 반환하지 않는다.
3. `ack_deadline_at` 초과 후 Gateway가 `BrokerCommandOutcomeUnknown(commandType=SUBMIT)` 발행한다.
4. Order Service가 주문을 `UNKNOWN`으로 전환한다.
5. Order Service가 `OrderReconciliationRequested(triggerType=SUBMIT_OUTCOME_UNKNOWN)` 발행한다.
6. Recovery Service가 reconciliation job을 생성한다.
7. 상태조회 snapshot에 따라 주문이 수렴한다.

성공 기준:

* 주문은 `REJECTED`로 임의 종결되지 않는다.
* `reconciliationStatus = PENDING`으로 전환된다.
* `broker_command_attempt.transport_state`는 `TIMED_OUT` 또는 `UNKNOWN`으로 기록된다.

---

## 12.7.8 cancel 결과 불확실

시나리오:

1. `LIVE` 또는 `PARTIALLY_FILLED` 주문에 취소 요청을 보낸다.
2. Gateway가 `CXLQ` 전문을 송신한다.
3. Broker Simulator가 CancelAck/CancelReject를 반환하지 않는다.
4. Gateway가 `BrokerCommandOutcomeUnknown(commandType=CANCEL)` 발행한다.
5. Order Service가 주문을 `UNKNOWN`으로 전환하고 active `CANCEL` instruction을 유지한다.
6. Recovery Service가 상태조회 job을 수행한다.
7. snapshot이 여전히 활성 상태이면 Order Service가 cancel command 재발행 여부를 결정한다.

성공 기준:

* timeout만으로 같은 cancel command를 Gateway가 직접 재전송하지 않는다.
* 사용자의 취소 의도는 사라지지 않는다.
* 상태조회 결과에 따라 최종 상태 또는 재취소 흐름으로 수렴한다.

---

## 12.7.9 malformed 전문 처리

| malformed 유형 | 기대 결과 |
| --- | --- |
| frame 길이 오류 | `broker_message_journal.parse_status = MALFORMED_FRAME`, 주문 상태 변경 없음 |
| header 파싱 오류 | `MALFORMED_HEADER`, 주문 상태 변경 없음 |
| body 파싱 오류 + pending command 매칭 | `BrokerCommandOutcomeUnknown` 가능 |
| body 파싱 오류 + 주문 식별 불가 | Gateway journal 기록, Order 이벤트 없음 |
| business semantic 오류 | 상태 반영 금지, anomaly 기록 |

성공 기준:

* 식별 불가능 malformed는 특정 주문을 직접 변경하지 않는다.
* pending command 결과가 불확실해진 경우에만 `UNKNOWN` 경로로 진입한다.
* malformed metric과 journal이 남는다.

---

## 12.7.10 stale non-terminal 탐지

시나리오:

1. 주문을 `LIVE`, `PARTIALLY_FILLED`, `PENDING_ACK`, `PENDING_CANCEL`, `UNKNOWN` 중 하나로 둔다.
2. 상태 변경 없이 stale threshold를 넘긴다.
3. Order Service stale detector가 대상 주문을 식별한다.
4. `OrderReconciliationRequested(triggerType=STALE_NON_TERMINAL)` 이벤트가 발행된다.

기대 결과:

| 기존 상태 | 기대 처리 |
| --- | --- |
| `PENDING_ACK` | `UNKNOWN + PENDING` |
| `PENDING_CANCEL` | `UNKNOWN + PENDING` |
| `LIVE` | 상태 유지 + `reconciliationStatus = PENDING` |
| `PARTIALLY_FILLED` | 상태 유지 + `reconciliationStatus = PENDING` |
| `UNKNOWN` | `reconciliationStatus = PENDING` 유지 또는 재요청 |

---

## 12.7.11 EOD non-terminal 탐지

시나리오:

1. Order Service의 시장 상태를 `CLOSED`로 전환한다.
2. `tif = DAY`인 non-terminal 주문을 준비한다.
3. EOD sweep을 실행한다.
4. Order Service가 `OrderReconciliationRequested(triggerType=EOD_NON_TERMINAL)`를 발행한다.

성공 기준:

* EOD sweep은 주문을 임의로 `EXPIRED`로 바꾸지 않는다.
* 기존 상태를 유지한 채 reconciliation 대상으로 표시한다.
* 실제 종결은 snapshot 또는 `BrokerOrderExpired` 이벤트로만 수행한다.

---

## 12.7.12 reconciliation workflow failure

시나리오:

1. `OrderReconciliationRequested` 이벤트를 발행한다.
2. Recovery Service가 job을 생성한다.
3. Broker Simulator가 상태조회 응답을 계속 timeout시킨다.
4. attempt retry 한도를 초과한다.
5. Recovery Service가 `ReconciliationJobFailed(failureType=ATTEMPT_RETRY_EXHAUSTED)` 발행한다.
6. Order Service가 `reconciliationStatus = FAILED`를 반영한다.

성공 기준:

* Recovery failure와 Order domain resolution failure가 구분된다.
* `reconciliation_job.failure_type`은 workflow 실패 사유만 저장한다.
* attempt별 실패 상세는 `reconciliation_attempt`에 남는다.

---

## 12.7.13 domain resolution failure

시나리오:

1. 상태조회 snapshot이 도착한다.
2. snapshot 상태가 `NOT_FOUND`이거나 수량 불변식을 위반한다.
3. Order Service가 snapshot을 주문 상태로 수렴시키지 못한다.
4. Order Service가 `OrderReconciliationFailed`를 발행한다.
5. Recovery Service가 job을 `FAILED`로 종료한다.

성공 기준:

* Order Service는 상태를 임의로 terminal 처리하지 않는다.
* 실패 사유는 `order_event.payload_json`에 남는다.
* Recovery Service는 이를 workflow failure로 재분류하지 않는다.

---

## 12.7.14 SSE 테스트

SSE는 주문 상태의 source of truth가 아니다.
따라서 테스트는 전송 실패가 주문 상태를 rollback하지 않는지 확인한다.

| 시나리오 | 기대 결과 |
| --- | --- |
| 상태 변경 후 SSE 연결 유지 | 클라이언트가 상태 변경 이벤트 수신 |
| SSE 연결 종료 | 주문 상태 변경은 DB에 유지 |
| 늦게 연결 | 조회 API로 현재 상태 확인 |
| SSE 이벤트 유실 | 조회 API 결과와 DB 상태가 source of truth |

성공 기준:

* SSE 실패는 주문 처리 실패로 기록하지 않는다.
* SSE latency는 별도 metric으로 측정한다.

---

## 12.8 초기 성능 / 지연 기준

이 수치는 프로덕션 SLO가 아니라 Phase 1 회귀 감지용 초기 목표다.

| 구간 | 초기 목표 | 측정 기준 |
| --- | ---: | --- |
| 주문 생성 API p95 | 200ms 이하 | HTTP server latency |
| 주문 조회 API p95 | 100ms 이하 | HTTP server latency |
| 주문 생성 후 `PENDING_ACK` 저장까지 p95 | 200ms 이하 | API 시작부터 DB commit |
| 브로커 ACK 수신 후 Order DB 상태 반영까지 p95 | 500ms 이하 | Gateway receive time부터 Order update |
| Order DB 상태 변경 후 SSE 전달까지 p95 | 1초 이하 | `order_event.created_at`부터 SSE write |
| submit/cancel outcome unknown 감지까지 p95 | 10초 이하 | command sent부터 unknown event |
| `UNKNOWN` 진입 후 reconciliation 완료까지 p95 | 60초 이하 | `OrderBecameUnknown`부터 resolved/failed |
| Outbox publish lag p95 | 5초 이하 | outbox created부터 published |
| Kafka consumer processing lag p95 | 5초 이하 | message timestamp부터 processed |

성능 테스트는 다음 조건에서 수행한다.

* 로컬 또는 단일 개발 환경
* 단일 브로커
* 지연 주입 없는 정상 흐름
* 소량 부하로 smoke test
* 실패 시 원인 분석을 위한 metric/log 수집

---

## 12.9 재시도 / Timeout / Parking 초기 정책

## 12.9.1 Broker command deadline

| command | deadline | timeout 처리 |
| --- | ---: | --- |
| `SUBMIT` | 5초 | `BrokerCommandOutcomeUnknown(commandType=SUBMIT, unknownReason=TIMEOUT)` |
| `CANCEL` | 5초 | `BrokerCommandOutcomeUnknown(commandType=CANCEL, unknownReason=TIMEOUT)` |
| `QUERY_STATUS` | 5초 | `StatusQueryAttemptReported(gatewayResult=TIMED_OUT)` |

주의:

* `SUBMIT`, `CANCEL` timeout은 같은 command 직접 재전송으로 이어지지 않는다.
* `QUERY_STATUS` timeout은 Recovery job/attempt 정책에 따라 재시도한다.

---

## 12.9.2 Outbox retry

| 항목 | 초기 정책 |
| --- | --- |
| 재시도 방식 | exponential backoff + jitter |
| 초기 지연 | 1초 |
| 최대 지연 | 5분 |
| 최대 재시도 횟수 | 12회 |
| lock timeout | 30초 |
| publisher polling interval | 1초 |
| 영구 실패 처리 | `FAILED` 유지 + 운영 경보 |

Backoff 예시:

| retry count | next delay 예 |
| ---: | ---: |
| 1 | 1초 |
| 2 | 2초 |
| 3 | 4초 |
| 4 | 8초 |
| 5 | 16초 |
| 6 | 32초 |
| 7 | 1분 |
| 8 이상 | 최대 5분 내 jitter |

중복 발행은 허용한다.
consumer idempotency가 이를 방어해야 한다.

---

## 12.9.3 Consumer retry와 parking

consumer는 메시지 처리 실패를 다음처럼 분류한다.

| 실패 유형 | 예 | 처리 |
| --- | --- | --- |
| 일시적 실패 | DB deadlock, 일시적 connection 오류 | message 재처리 허용 |
| 중복 message | 같은 `messageId` 재소비 | `processed_message` 기준 skip |
| schema parse 불가 | envelope JSON 파싱 실패 | parking 대상 |
| 도메인 규칙상 처리 불가 | terminal 오염 가능 이벤트 | 상태 반영 금지, anomaly 기록 |
| 반복 실패 | 같은 message가 계속 실패 | parking 대상 |

Phase 1 parking 정책:

| 항목 | 초기 정책 |
| --- | --- |
| parking 방식 | Phase 1은 서비스별 DB `parked_message` log 우선, 필요 시 별도 parking topic 검토 |
| parking topic naming | 별도 topic 선택 시 `<source-topic>.parking.v1` |
| parking payload | 원본 envelope 또는 raw payload, error code, error message, consumer name, source topic, trace id, failed at, parked at |
| 반복 실패 기준 | 같은 consumer에서 5회 실패 |
| non-retryable parse 오류 | 즉시 parking |
| parking 후 처리 | offset commit 후 운영 조사 대상으로 전환 |

parking은 자동 복구 큐가 아니라 **운영 격리 지점**이다.
parking된 메시지를 재주입하는 도구는 Phase 2에서 검토한다.
known envelope이 반복 실패로 여러 번 parking될 수 있으므로 M2에서는 중복 row를 운영 감사 로그로 허용한다.
schema parse 실패처럼 envelope field를 추출할 수 없는 경우 `trace_id`와 envelope identity는 NULL로 둔다.
raw payload가 NULL이면 `payload_text`에는 `"<null>"` sentinel을 저장하고, error message가 NULL이면 `error_code`를 fallback으로 저장한다.
512자를 초과하는 error message는 저장 전 512자로 잘라낸다.
현재 Phase 1 API가 실패 시각과 parking 기록 시각을 별도로 받지 않는 경우 `failed_at`과 `parked_at`은 같은 값으로 저장할 수 있다.

---

## 12.9.4 Reconciliation retry

| 항목 | 초기 정책 |
| --- | --- |
| 최대 attempt 수 | 4회 |
| attempt deadline | 10초 |
| retry backoff | 0초, 5초, 15초, 30초 |
| job claim lock timeout | 30초 |
| active job 중복 생성 | 금지 |
| 한도 초과 | `ReconciliationJobFailed(ATTEMPT_RETRY_EXHAUSTED)` |

attempt가 실패해도 주문 상태를 직접 변경하지 않는다.
최종 변경은 Order Service가 `BrokerOrderStatusSnapshot`, `OrderReconciliationFailed`, `ReconciliationJobFailed`를 처리하면서 수행한다.

---

## 12.9.5 Stale / EOD sweep

| 항목 | 초기 정책 |
| --- | --- |
| stale sweep interval | 30초 |
| `PENDING_ACK` stale threshold | 30초 |
| `PENDING_CANCEL` stale threshold | 30초 |
| `LIVE` stale threshold | 5분 |
| `PARTIALLY_FILLED` stale threshold | 5분 |
| `UNKNOWN` pending 재요청 threshold | 1분 |
| EOD sweep interval | 시장 상태 `CLOSED` 전환 후 1분마다 |
| EOD 대상 | `tif = DAY` + non-terminal |

Phase 1의 threshold는 실험용 초기값이다.
실제 시장/브로커 latency를 반영한 운영 SLO로 보지 않는다.

---

## 12.10 모니터링 지표

## 12.10.1 지표 명명 원칙

메트릭 이름은 다음 형식을 따른다.

```text
trading_<domain>_<metric>_<unit>
```

예:

```text
trading_order_created_total
trading_gateway_command_timeout_total
trading_recovery_job_duration_seconds
```

label은 과도하게 늘리지 않는다.
`orderId`, `messageId`, `wireMessageId`처럼 cardinality가 높은 값은 metric label로 사용하지 않고 log/tracing 필드로 남긴다.

---

## 12.10.2 Order Service metrics

| Metric | Type | 주요 label | 의미 |
| --- | --- | --- | --- |
| `trading_order_created_total` | Counter | `orderType`, `tif` | 주문 생성 수 |
| `trading_order_status_transition_total` | Counter | `fromStatus`, `toStatus`, `reason` | 상태 전이 수 |
| `trading_order_current_status_count` | Gauge | `status` | 상태별 현재 주문 수 |
| `trading_order_reconciliation_status_count` | Gauge | `reconciliationStatus` | reconciliation 상태별 주문 수 |
| `trading_order_unknown_entered_total` | Counter | `triggerType` | `UNKNOWN` 진입 수 |
| `trading_order_event_duplicate_total` | Counter | `eventType` | dedup된 외부 사건 수 |
| `trading_order_state_transition_rejected_total` | Counter | `eventType`, `reason` | 상태머신이 거부한 이벤트 |
| `trading_order_invariant_violation_total` | Counter | `invariantType` | 수량/상태 불변식 위반 |
| `trading_order_stale_detected_total` | Counter | `status` | stale 탐지 수 |
| `trading_order_eod_non_terminal_total` | Counter | `status` | EOD non-terminal 탐지 수 |
| `trading_order_api_latency_seconds` | Histogram | `endpoint`, `method`, `status` | API latency |
| `trading_order_sse_delivery_latency_seconds` | Histogram | `eventType` | SSE 전달 지연 |
| `trading_order_sse_connection_count` | Gauge | 없음 | 현재 SSE 연결 수 |

---

## 12.10.3 Broker Gateway metrics

| Metric | Type | 주요 label | 의미 |
| --- | --- | --- | --- |
| `trading_gateway_command_sent_total` | Counter | `commandType`, `brokerCode` | 브로커 command 송신 수 |
| `trading_gateway_command_timeout_total` | Counter | `commandType`, `brokerCode` | command timeout 수 |
| `trading_gateway_command_failed_total` | Counter | `commandType`, `errorCode` | 전송/처리 실패 수 |
| `trading_gateway_command_latency_seconds` | Histogram | `commandType`, `brokerCode` | command 응답 latency |
| `trading_gateway_broker_event_published_total` | Counter | `eventType` | canonical event 발행 수 |
| `trading_gateway_malformed_message_total` | Counter | `parseStatus`, `msgId` | malformed 전문 수 |
| `trading_gateway_journal_recorded_total` | Counter | `direction`, `parseStatus` | journal 기록 수 |
| `trading_gateway_connection_state` | Gauge | `brokerCode` | TCP 연결 상태 |
| `trading_gateway_reconnect_total` | Counter | `brokerCode` | reconnect 수 |
| `trading_gateway_status_query_report_total` | Counter | `gatewayResult` | 상태조회 report 수 |

`msgId` label은 값 종류가 제한적인 전문 ID에만 사용한다.
원문 전문, `wireMessageId`, `orderId`는 label에 넣지 않는다.

---

## 12.10.4 Recovery Service metrics

| Metric | Type | 주요 label | 의미 |
| --- | --- | --- | --- |
| `trading_recovery_job_created_total` | Counter | `triggerType` | job 생성 수 |
| `trading_recovery_job_current_status_count` | Gauge | `status`, `triggerType` | job 상태별 수 |
| `trading_recovery_attempt_created_total` | Counter | `triggerType` | attempt 생성 수 |
| `trading_recovery_attempt_result_total` | Counter | `resultStatus`, `gatewayResult` | attempt 결과 수 |
| `trading_recovery_job_duration_seconds` | Histogram | `triggerType`, `result` | job 처리 시간 |
| `trading_recovery_job_failed_total` | Counter | `failureType` | workflow failure 수 |
| `trading_recovery_retry_scheduled_total` | Counter | `triggerType` | 재시도 예약 수 |
| `trading_recovery_active_job_conflict_total` | Counter | `triggerType` | active job 중복 생성 방지 수 |

---

## 12.10.5 Messaging / Outbox metrics

| Metric | Type | 주요 label | 의미 |
| --- | --- | --- | --- |
| `trading_outbox_message_current_status_count` | Gauge | `service`, `status`, `topic` | outbox 상태별 수 |
| `trading_outbox_publish_attempt_total` | Counter | `service`, `topic`, `result` | 발행 시도 수 |
| `trading_outbox_publish_lag_seconds` | Histogram | `service`, `topic` | 생성부터 발행까지 지연 |
| `trading_outbox_oldest_ready_age_seconds` | Gauge | `service`, `topic` | 가장 오래된 미발행 메시지 나이 |
| `trading_consumer_processed_total` | Counter | `service`, `consumerName`, `messageType` | 처리 성공 수 |
| `trading_consumer_duplicate_total` | Counter | `service`, `consumerName`, `messageType` | envelope 중복 수 |
| `trading_consumer_failed_total` | Counter | `service`, `consumerName`, `errorType` | 처리 실패 수 |
| `trading_consumer_parking_total` | Counter | `service`, `consumerName`, `sourceTopic` | parking 수 |
| `trading_kafka_consumer_lag` | Gauge | `consumerGroup`, `topic`, `partition` | Kafka lag |

---

## 12.11 구조화 로그

## 12.11.1 공통 로그 필드

모든 서비스는 주요 이벤트 로그에 다음 필드를 포함한다.

| 필드 | 설명 |
| --- | --- |
| `timestamp` | 로그 시각 |
| `level` | 로그 레벨 |
| `service` | 서비스명 |
| `component` | 컴포넌트명 |
| `eventName` | 로그 이벤트명 |
| `traceId` | 요청/메시지 추적 ID |
| `orderId` | 주문 ID |
| `messageId` | 내부 message envelope ID |
| `messageType` | 메시지 타입 |
| `errorCode` | 오류 코드 |
| `errorMessage` | 요약 오류 메시지 |

민감 정보와 raw payload 전체를 일반 로그에 남기지 않는다.
브로커 원문 전문은 `broker_message_journal`에 저장하고, 로그에는 journal ID와 요약만 남긴다.

---

## 12.11.2 서비스별 추가 로그 필드

| 서비스 | 추가 필드 |
| --- | --- |
| Order Service | `clientOrderId`, `clientCancelRequestId`, `instructionId`, `fromStatus`, `toStatus`, `reconciliationStatus`, `dedupKey` |
| Broker Gateway | `brokerCode`, `commandType`, `wireMessageId`, `msgId`, `direction`, `parseStatus`, `journalId`, `transportState` |
| Recovery Service | `jobId`, `attemptId`, `triggerType`, `resultStatus`, `failureType`, `gatewayResult`, `snapshotStatus` |
| Outbox Publisher | `outboxMessageId`, `topicName`, `retryCount`, `nextRetryAt`, `lockedBy` |
| Consumer | `consumerName`, `topic`, `partition`, `offset`, `processedMessageKey` |

---

## 12.11.3 로그 레벨 기준

| 상황 | Level | 기준 |
| --- | --- | --- |
| 정상 상태 전이 | INFO | 요약 필드만 기록 |
| 정상 message 처리 | DEBUG 또는 INFO | 개발 환경 INFO, 운영 환경 DEBUG 가능 |
| 중복 message/event | INFO | 장애는 아니지만 추적 가능해야 함 |
| 사용자 요청 검증 실패 | INFO | 사용자 입력 오류 |
| command timeout | WARN | 복구 대상 |
| malformed 전문 | WARN | 주문 상태 직접 변경 금지 |
| outbox publish 실패 | WARN | 재시도 가능 |
| outbox 영구 실패 | ERROR | 운영 개입 필요 |
| reconciliation workflow failure | ERROR | 운영 개입 가능 |
| domain resolution failure | ERROR | 주문별 조사 필요 |
| 상태 불변식 위반 | ERROR | 즉시 조사 대상 |

---

## 12.12 Tracing 계획

Phase 1에서는 OpenTelemetry 같은 표준 tracing 도입을 권장하지만, 구현이 늦어져도 `traceId` 전파는 먼저 보장한다.

## 12.12.1 traceId 전파 경로

```text
Client Request
  -> Order Service API
  -> Outbox Message
  -> Kafka Envelope
  -> Broker Gateway
  -> Broker TCP Journal
  -> Canonical Broker Event
  -> Order Service Consumer
  -> Order Lifecycle Event
  -> Recovery Service
  -> Query Status Command
  -> Broker Gateway
  -> Status Query Attempt Report
```

원칙:

* 외부 요청에 `traceId`가 없으면 Order Service가 생성한다.
* 모든 내부 message envelope는 `traceId`를 포함한다.
* Gateway는 `traceId`를 `broker_command_attempt`와 `broker_message_journal`에 기록한다.
* Recovery Service는 job/attempt 로그에 `traceId`가 없더라도 `orderId`, `jobId`, `attemptId`로 추적 가능해야 한다.

---

## 12.12.2 Span 후보

| 서비스 | Span |
| --- | --- |
| Order Service | `OrderApi.createOrder`, `OrderApi.cancelOrder`, `OrderState.applyBrokerEvent`, `OrderReconciliation.applySnapshot` |
| Broker Gateway | `Gateway.consumeCommand`, `Gateway.sendTcpCommand`, `Gateway.parseBrokerMessage`, `Gateway.publishBrokerEvent` |
| Recovery Service | `Recovery.consumeReconciliationRequested`, `Recovery.createAttempt`, `Recovery.publishQueryStatus`, `Recovery.completeJob` |
| Outbox Publisher | `Outbox.claim`, `Outbox.publish`, `Outbox.markSent` |
| Consumer | `Consumer.deserialize`, `Consumer.idempotencyCheck`, `Consumer.handle`, `Consumer.commit` |

---

## 12.13 Dashboard 구성

## 12.13.1 Order Overview Dashboard

목적:

* 주문 상태와 사용자-facing 처리 흐름을 한눈에 본다.

패널:

* 상태별 주문 수
* reconciliation 상태별 주문 수
* 주문 생성/취소 요청 수
* 주문 생성 API latency p50/p95/p99
* 주문 조회 API latency p50/p95/p99
* 상태 전이 수
* `UNKNOWN` 진입 수
* 상태 전이 거부 / 불변식 위반 수
* SSE 연결 수와 delivery latency

---

## 12.13.2 Broker Gateway Dashboard

목적:

* 브로커 통신과 전문 파싱 상태를 본다.

패널:

* command type별 송신 수
* command latency p50/p95/p99
* command timeout 수
* TCP connection state
* reconnect 수
* malformed 전문 수
* parse status별 journal 수
* broker event 발행 수
* `BrokerCommandOutcomeUnknown` 발행 수
* 상태조회 report 결과 분포

---

## 12.13.3 Recovery Dashboard

목적:

* reconciliation workflow가 밀리거나 실패하는지 본다.

패널:

* trigger type별 job 생성 수
* job 상태별 현재 수
* active job 수
* job duration p50/p95/p99
* attempt 결과 분포
* retry 예약 수
* workflow failure 수
* domain resolution failure 수
* oldest pending job age

---

## 12.13.4 Messaging Dashboard

목적:

* 메시지 발행/소비 신뢰성 문제를 본다.

패널:

* 서비스별 outbox status count
* oldest READY/FAILED outbox age
* publish failure rate
* consumer lag
* consumer failure count
* processed duplicate count
* parking count
* topic별 message throughput

---

## 12.13.5 Reliability Scenario Dashboard

목적:

* 장애 시나리오 테스트 실행 결과를 비교한다.

패널:

* scenario별 성공/실패
* scenario별 최종 주문 상태
* unknown 진입 후 reconciliation 완료 시간
* malformed 주입 수와 격리 결과
* duplicate 주입 수와 dedup 결과
* 순서 역전 scenario의 상태 수렴 결과

이 dashboard는 운영보다는 개발/테스트 분석용이다.

---

## 12.14 Alert 초기 기준

Phase 1의 alert는 로컬/개발 환경 기준이므로 "즉시 호출"이 아니라 "확인 필요" 수준이다.

## 12.14.1 Critical

| 조건 | 기준 | 이유 |
| --- | --- | --- |
| 상태 불변식 위반 | 1건 이상 | 주문 수량/상태 오염 가능성 |
| outbox 영구 실패 | 1건 이상 | 메시지 발행 유실 위험 |
| domain resolution failure | 1건 이상 | 주문별 수동 조사 필요 |
| `processed_message` 저장 실패 | 1건 이상 | 중복 처리 방어 약화 |
| DB migration 실패 | 1건 이상 | 서비스 정상 동작 불가 |

---

## 12.14.2 Warning

| 조건 | 기준 | 이유 |
| --- | --- | --- |
| `UNKNOWN` 진입 증가 | 5분 10건 이상 | 브로커 응답 문제 가능 |
| command timeout rate | 5분간 command의 10% 이상 | Gateway 또는 브로커 지연 |
| malformed 전문 증가 | 5분 5건 이상 | protocol 문제 가능 |
| oldest outbox READY age | 1분 초과 | publisher 지연 가능 |
| oldest outbox FAILED age | 5분 초과 | 반복 publish 실패 |
| Kafka consumer lag | 1분 이상 증가 지속 | consumer 처리 지연 |
| reconciliation pending job age | 1분 초과 | Recovery 지연 |
| reconciliation failure rate | 10분간 10% 이상 | 상태조회 또는 domain resolution 문제 |
| SSE delivery p95 | 3초 초과 | 사용자 인지 지연 |

---

## 12.14.3 Info

| 조건 | 기준 | 이유 |
| --- | --- | --- |
| duplicate event 증가 | 5분 20건 이상 | 브로커 재전송 또는 테스트 주입 확인 |
| active cancel conflict | 5분 5건 이상 | 사용자 재시도/UX 확인 |
| parking 발생 | 1건 이상 | 격리 메시지 조사 필요 |
| Gateway reconnect | 5분 3건 이상 | 브로커 연결 안정성 확인 |

---

## 12.15 운영 Runbook

## 12.15.1 특정 주문 상태 이상 조사

입력:

* `orderId`
* 가능하면 `traceId`, `clientOrderId`, `clientCancelRequestId`

조사 순서:

1. `trade_order`에서 현재 `status`, `reconciliationStatus`, `version`, `cumQty`, `leavesQty` 확인
2. `order_instruction`에서 active instruction 확인
3. `order_event`를 `created_at` 순으로 조회
4. `order_event.dedup_key` 중복 또는 상태 전이 거부 이력 확인
5. Gateway의 `broker_command_attempt`를 `orderId` 기준으로 조회
6. `wireMessageId`로 `broker_message_journal` 송수신 전문 확인
7. `reconciliation_job`과 `reconciliation_attempt` 확인
8. 관련 outbox message가 `FAILED` 또는 오래된 `READY`인지 확인
9. 관련 consumer parking 또는 error log 확인
10. 최종적으로 workflow failure인지 domain resolution failure인지 분류

---

## 12.15.2 `UNKNOWN` 주문 증가 조사

확인 순서:

1. `UNKNOWN` 진입 trigger type 분포 확인
2. Gateway command timeout metric 확인
3. Broker Simulator 또는 브로커 연결 상태 확인
4. malformed metric 증가 여부 확인
5. outbox backlog로 command 발행이 지연됐는지 확인
6. Recovery pending job age 확인
7. `OrderReconciliationResolved` / `OrderReconciliationFailed` 비율 확인

판단:

| 증상 | 가능한 원인 |
| --- | --- |
| `SUBMIT_OUTCOME_UNKNOWN` 급증 | 브로커 ACK 지연, Gateway timeout 설정 문제 |
| `CANCEL_OUTCOME_UNKNOWN` 급증 | 취소 응답 지연, cancel 전문 파싱 문제 |
| `STALE_NON_TERMINAL` 급증 | terminal event 유실, consumer 지연 |
| `EOD_NON_TERMINAL` 급증 | 만료 이벤트 유실, 시장 상태 전환 시점 문제 |

---

## 12.15.3 Outbox backlog 조사

확인 순서:

1. 서비스별 `outbox_message.status` 분포 확인
2. 가장 오래된 `READY`, `FAILED` 메시지 확인
3. `last_error`, `retry_count`, `next_retry_at` 확인
4. Kafka broker 상태 확인
5. publisher lock이 만료되지 않고 남아 있는지 확인
6. 같은 aggregate의 후속 메시지가 지연되고 있는지 확인

조치:

* lock 만료 후 자동 회수되는지 확인한다.
* `FAILED`가 최대 재시도 한도에 도달했다면 운영 조사 대상으로 남긴다.
* 수동 재발행 기능은 Phase 2에서 별도 설계한다.

---

## 12.15.4 malformed 전문 조사

확인 순서:

1. `trading_gateway_malformed_message_total`의 `parseStatus` 확인
2. `broker_message_journal`에서 malformed journal 조회
3. `wireMessageId`, `msgId`, `payload_hash` 확인
4. pending command와 연결되는지 확인
5. 연결된다면 `BrokerCommandOutcomeUnknown` 발행 여부 확인
6. 연결되지 않는다면 주문 상태가 직접 변경되지 않았는지 확인
7. 이후 stale/EOD detector가 관련 주문을 간접 복구하는지 확인

조치 기준:

| 상황 | 조치 |
| --- | --- |
| pending command 응답 malformed | outcome unknown + reconciliation 흐름 확인 |
| order 식별 불가 | journal/metric 확인, 직접 상태 변경 없음 확인 |
| 반복 malformed | Broker Simulator scenario 또는 Gateway parser regression 확인 |

---

## 12.15.5 Reconciliation 실패 조사

먼저 실패 유형을 구분한다.

| 실패 유형 | 소유 서비스 | 대표 이벤트 |
| --- | --- | --- |
| workflow failure | Recovery Service | `ReconciliationJobFailed` |
| domain resolution failure | Order Service | `OrderReconciliationFailed` |

workflow failure 조사:

1. `reconciliation_job.failure_type` 확인
2. attempt 개수와 각 `result_status` 확인
3. `StatusQueryAttemptReported.gatewayResult` 확인
4. Gateway 상태조회 command timeout 또는 malformed 여부 확인
5. outbox 발행 실패 여부 확인

domain resolution failure 조사:

1. `order_event.payload_json`의 실패 사유 확인
2. snapshot 상태 확인
3. 수량 불변식 위반 여부 확인
4. 현재 주문 terminal 상태와 snapshot 충돌 여부 확인
5. `NOT_FOUND`인지 확인

`NOT_FOUND`는 자동 종결하지 않는다.
운영자가 실제 주문 미도달로 판단하더라도 Phase 1에서는 별도 수동 종결 기능 없이 실패 이력으로 남긴다.

---

## 12.16 운영 / 테스트용 Admin 기능

Phase 1에서 필요한 admin 기능은 운영 콘솔 UI가 아니라 API 또는 command 형태로 제공해도 된다.

| 기능 | 대상 서비스 | 목적 | Phase 1 |
| --- | --- | --- | --- |
| 시장 상태 `OPEN/CLOSED` 전환 | Order Service | EOD 테스트 | Must |
| stale sweep 수동 실행 | Order Service | stale 탐지 테스트 | Should |
| EOD sweep 수동 실행 | Order Service | EOD 테스트 | Should |
| reconciliation 수동 요청 | Order Service | 특정 주문 복구 재시도 | Should |
| reconciliation job abort | Recovery Service | stuck job 정리 | Could |
| Broker Simulator scenario 설정 | Broker Simulator | 장애 주입 | Must |
| Broker Simulator 상태 초기화 | Broker Simulator | 테스트 격리 | Must |
| outbox 상태 조회 | 각 서비스 | 운영 추적 | Should |
| parking message 조회 | 각 consumer 또는 운영 도구 | poison 조사 | Should |

보안 기준:

* admin 기능은 로컬/테스트 환경 전용으로 둔다.
* 일반 사용자 API와 endpoint prefix를 분리한다.
* 운영 API 요청은 구조화 로그로 남긴다.

---

## 12.17 운영 데이터 보존

Phase 1에서는 장기 보존보다 디버깅 가능성을 우선한다.

| 데이터 | 기본 보존 기준 | 비고 |
| --- | ---: | --- |
| `trade_order` | 영구 | 테스트 정리 대상 제외 시 유지 |
| `order_instruction` | 영구 | 주문 이력 일부 |
| `order_event` | 영구 | 상태 변화 원인 분석 핵심 |
| `broker_command_attempt` | 30일 | 개발 환경에서는 수동 정리 가능 |
| `broker_message_journal` | 7일 | 원문 전문 저장 비용 고려 |
| `reconciliation_job` | 30일 | 복구 이력 |
| `reconciliation_attempt` | 30일 | 복구 상세 |
| `outbox_message SENT` | 7일 | 운영 추적 후 정리 가능 |
| `outbox_message FAILED` | 조사 완료 전 보존 | 운영 개입 필요 |
| `processed_message` | 7일 | 중복 재전달 window 기준 |
| `parked_message` | 30일 또는 조사 완료 후 정리 | poison message 운영 감사 로그 |
| application logs | 7일 | 로컬 환경 기준 |
| metrics | 7일 | 회귀 분석용 |

보존 정책은 실제 구현 시 batch cleanup job으로 분리한다.
Phase 1에서는 cleanup 자동화보다 조회 가능성을 우선한다.

---

## 12.18 배포 전 점검 체크리스트

Phase 1에서 서비스 실행 또는 milestone 완료 전 다음을 확인한다.

## 12.18.1 기능 체크

* 주문 생성 API가 `PENDING_ACK` 주문과 `PLACE` instruction을 생성한다.
* 주문 취소 API가 허용 상태에서 `CANCEL` instruction을 생성한다.
* Order Service는 브로커 canonical event로만 주문 상태를 변경한다.
* Gateway는 브로커 전문을 journal에 기록한다.
* Recovery Service는 reconciliation job/attempt를 기록한다.
* SSE는 상태 변경을 전송하지만 source of truth가 아니다.

---

## 12.18.2 신뢰성 체크

* Outbox가 업무 transaction과 함께 저장된다.
* publisher 장애 후 lock 만료로 재claim 가능하다.
* consumer는 `processed_message`로 envelope 중복을 방어한다.
* Order Service는 `brokerEventDedupKey`로 외부 사건 중복을 방어한다.
* submit/cancel timeout은 직접 재전송하지 않고 `UNKNOWN`으로 격리한다.
* query status timeout은 Recovery attempt로 재시도한다.
* `NOT_FOUND` snapshot은 자동 terminal 처리하지 않는다.

---

## 12.18.3 관측성 체크

* 주요 API latency metric이 수집된다.
* 주문 상태별 count metric이 수집된다.
* Gateway command timeout과 malformed metric이 수집된다.
* Recovery job/attempt metric이 수집된다.
* Outbox backlog metric이 수집된다.
* 로그에 `traceId`, `orderId`, `wireMessageId`, `jobId`가 포함된다.
* dashboard에서 `UNKNOWN` 증가와 reconciliation 실패를 볼 수 있다.

---

## 12.18.4 테스트 체크

* 상태머신 unit test가 주요 전이를 검증한다.
* Gateway parser test가 malformed 유형을 검증한다.
* 멱등성 repository/component test가 있다.
* Broker Simulator 장애 주입 test가 있다.
* 중복 이벤트, 순서 역전, timeout, malformed, stale/EOD scenario test가 있다.
* 초기 latency smoke test가 있다.
* 운영 runbook을 따라 특정 주문 장애를 재현 조사할 수 있다.

---

## 12.19 Phase 1 구현 우선순위

## Must

| 항목 | 이유 |
| --- | --- |
| 주문 상태머신 unit test | 상태 수렴의 핵심 |
| Gateway parser/serializer test | 전문 오류가 상태를 오염시키지 않도록 방어 |
| 주문/취소 멱등성 test | 사용자 재시도 안전성 |
| Outbox publish retry test | 메시지 유실 방지 |
| consumer idempotency test | at-least-once 메시징 방어 |
| submit/cancel timeout -> `UNKNOWN` test | 핵심 복구 경로 |
| reconciliation success/failure test | 복구 수렴 검증 |
| malformed 격리 test | 장애 격리 |
| 기본 metrics/log fields | 운영 추적 |

---

## Should

| 항목 | 이유 |
| --- | --- |
| SSE latency test | 사용자 인지 지연 확인 |
| stale/EOD 자동 sweep test | 방치 주문 방지 |
| parking topic/log 구현 | poison message 격리 |
| dashboard 구성 | 운영 가시성 |
| manual reconciliation trigger | 실험 편의 |
| performance smoke test | 회귀 감지 |

---

## Could

| 항목 | 이유 |
| --- | --- |
| 운영 콘솔 UI | 조회 편의 |
| parking message replay | 수동 복구 자동화 |
| chaos test 자동화 | 반복 장애 실험 |
| long-running soak test | 누적 lag와 memory leak 확인 |
| OpenTelemetry full tracing | 상세 분산 추적 |

---

## Won't Have in Phase 1

| 항목 | 제외 이유 |
| --- | --- |
| 실제 운영 paging/on-call | 개인 프로젝트 범위 초과 |
| 자동 주문 수동 종결 UI | 잘못된 종결 위험 |
| parking message 자동 재처리 | 안전한 재주입 정책 필요 |
| 멀티 브로커 운영 dashboard | Phase 2 확장 범위 |
| 실제 인증/인가 기반 운영 콘솔 | Phase 1 최소 보안 범위 |

---

## 12.20 정책 요약

| 영역 | 결정 |
| --- | --- |
| 테스트 중심 | 상태 수렴, 멱등성, 장애 격리, 복구 가능성 |
| 핵심 테스트 도구 | Broker Simulator scenario injection |
| command deadline | `SUBMIT`/`CANCEL`/`QUERY_STATUS` 5초 |
| Outbox retry | exponential backoff + jitter, 최대 12회 |
| Consumer parking | parse 불가 즉시, 반복 실패 5회 |
| Reconciliation retry | 최대 4 attempts, 0초/5초/15초/30초 backoff |
| stale threshold | pending 30초, live/partial 5분 |
| EOD 처리 | terminal 임의 변경 금지, reconciliation 요청 |
| metric cardinality | `orderId`, `wireMessageId`는 label 금지 |
| 로그 기준 | 구조화 로그, `traceId`/`orderId` 중심 |
| dashboard | Order, Gateway, Recovery, Messaging, Scenario |
| alert 기준 | 불변식 위반, outbox 영구 실패, reconciliation failure 우선 |
| 운영 조사 | `orderId`에서 event, command, journal, job, attempt로 확장 |
