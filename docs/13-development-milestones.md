# 13. 개발 마일스톤

## 13.1 목적

이 문서는 앞선 설계를 실제 구현 순서로 전환하기 위한 개발 마일스톤을 정의한다.

앞선 문서가 "무엇을 만들 것인가"와 "왜 그렇게 설계했는가"를 다뤘다면, 이 문서는 다음 질문에 답한다.

1. 어떤 순서로 구현해야 핵심 위험을 빨리 줄일 수 있는가?
2. 각 단계가 끝났다는 기준은 무엇인가?
3. 어떤 기능을 먼저 얇게 만들고, 어떤 기능을 뒤로 미룰 것인가?
4. 통합 테스트와 신뢰성 시나리오를 어느 시점부터 실행할 것인가?
5. Phase 1 완료를 어떤 기준으로 판단할 것인가?

---

## 13.2 마일스톤 구성 방식

Phase 1 개발은 **신뢰성 시나리오 중심 세로 슬라이스 방식**으로 진행한다.

즉, 특정 서비스를 끝까지 완성한 뒤 다음 서비스로 넘어가기보다, 다음과 같은 사용자/시스템 흐름을 얇게 먼저 관통시킨다.

```text
주문 생성
  -> 메시지 발행
  -> Broker Gateway
  -> Broker Simulator
  -> canonical broker event
  -> Order Service 상태 반영
```

이후 중복 이벤트, 순서 역전, 취소 경합, `UNKNOWN`, reconciliation, malformed 전문을 시나리오 단위로 확장한다.

이 방식을 선택한 이유는 이 프로젝트의 핵심 가치가 단순 기능 완성이 아니라 **주문 상태 보존, 중복 방어, 불확실성 격리, 최종 상태 수렴**을 증명하는 데 있기 때문이다.

---

## 13.3 초기 마일스톤 방향

세로 슬라이스 방식으로 진행하되, 실제 개발을 시작할 수 있는 최소 기반은 먼저 만든다.

따라서 초기 순서는 다음과 같이 둔다.

| Milestone | 이름 | 목적 |
| --- | --- | --- |
| `M0` | 프로젝트 기반 | multi-module, 로컬 실행 환경, 테스트/CI 기반을 최소 수준으로 준비한다. |
| `M1` | Order 도메인 코어 | 주문 상태의 source of truth가 될 상태머신, 수량 불변식, 기본 API와 DB를 구현한다. |
| `M2` | Reliable messaging 기반 | Kafka envelope, Outbox, processed message, traceId 전파를 구현해 비동기 처리 신뢰성의 기반을 만든다. |
| `M3` | Broker protocol과 Simulator | TCP fixed-length 전문 parser/serializer와 Netty 기반 Broker Simulator를 구현한다. |
| `M4` | 정상 주문 end-to-end | 주문 생성부터 Gateway, Simulator, ACK/Reject/Fill event, Order 상태 반영까지 관통한다. |
| `M5` | 기본 취소 흐름 | 취소 요청, 취소 멱등성, Gateway 취소 전문, 취소 완료/거절 반영을 구현한다. |
| `M6` | 부분체결 후 취소와 경합 | 부분체결 주문의 취소, 취소 중 추가 체결, 취소 거절 후 수량 기준 수렴을 구현한다. |
| `M7` | Submit UNKNOWN과 Reconciliation | 주문 요청 결과 불확실성을 `UNKNOWN`으로 격리하고 상태조회 기반으로 수렴시킨다. |
| `M8` | Cancel UNKNOWN과 Reconciliation | 취소 요청 결과 불확실성을 `UNKNOWN`으로 격리하고 취소 의도를 유지한 채 수렴시킨다. |
| `M9` | 중복과 순서 역전 hardening | 중복 메시지/이벤트와 순서 역전 이벤트가 주문 상태를 오염시키지 않도록 고정한다. |
| `M10` | Malformed / Stale / EOD hardening | malformed 전문 격리, stale non-terminal 탐지, EOD reconciliation 대상을 구현한다. |
| `M11` | 관측성과 운영 준비 | metrics, structured logs, traceId 전파 점검, 운영 조회와 runbook을 준비한다. |
| `M12` | Phase 1 안정화 | 전체 회귀 테스트, 성능 smoke, 문서 동기화, Phase 2 backlog 정리를 수행한다. |

`M0`에서는 공통 추상화나 운영 인프라를 과하게 만들지 않는다.
목표는 “개발을 시작할 수 있는 골격”이지 “완성된 플랫폼 기반”이 아니다.

`M1`에서는 아직 Broker Gateway, Broker Simulator, Kafka end-to-end 흐름을 완성하지 않는다.
대신 이후 세로 슬라이스의 중심이 될 Order 상태 모델과 도메인 규칙을 먼저 안정화한다.

`M2`에서는 실제 브로커 통신보다 메시지 발행/소비 신뢰성 기반을 먼저 만든다.
정상 주문 흐름이 만들어지는 시점부터 Outbox와 processed message가 포함되도록 하기 위함이다.

`M3`에서는 외부 브로커 연동 경계를 구현한다.
이 단계의 목적은 실제 주문 end-to-end 완성이 아니라, Gateway와 Simulator가 공유할 TCP 전문 계약과 장애 시나리오 주입 기반을 먼저 검증하는 것이다.

`M4`에서는 첫 번째 세로 슬라이스를 완성한다.
범위는 ACK만으로 제한하지 않고, 주문 생성 계열의 핵심 결과인 접수, 거절, 부분체결, 완전체결까지 포함한다.
취소 흐름은 별도 마일스톤으로 분리한다.

`M5`에서는 취소 흐름을 작게 관통한다.
범위는 `LIVE` 주문의 취소 요청, `PENDING_ACK` 주문의 취소 요청 접수, 취소 요청 멱등성, active cancel 중복 방지, Gateway `CXLQ` 송신, `CXLA` / `CXLR` 반영까지다.
부분체결 후 취소 경합과 취소 중 추가 체결은 이후 마일스톤으로 분리한다.

`M6`에서는 부분체결 이후 취소에서 발생하는 상태 조합을 다룬다.
범위는 `PARTIALLY_FILLED` 주문의 취소 요청, 이미 체결된 수량 유지, 미체결 잔량 취소, 취소 대기 중 추가 체결, 취소 대기 중 전량 체결, 취소 거절 후 수량 기준 상태 수렴까지다.
`UNKNOWN`과 reconciliation은 이 상태 조합이 안정화된 뒤 구현한다.

`M7`에서는 주문 생성 command의 결과 불확실성을 먼저 다룬다.
범위는 `ORDR` 전송 후 ACK/Reject timeout, `PENDING_ACK -> UNKNOWN`, `OrderReconciliationRequested`, Recovery job 생성, 상태조회 command, broker snapshot 수신, Order Service의 snapshot 기반 상태 수렴까지다.
취소 command 결과 불확실성은 별도 마일스톤으로 분리한다.

`M8`에서는 취소 command의 결과 불확실성을 다룬다.
범위는 `CXLQ` 전송 후 CancelAck/CancelReject timeout, `PENDING_CANCEL -> UNKNOWN`, active `CANCEL` instruction 유지, 상태조회 snapshot 수렴, 필요한 경우 취소 의도에 따른 후속 cancel command 재발행 판단까지다.
특히 `PENDING_ACK` 상태에서 취소가 접수된 주문은 submit 결과와 cancel 의도가 함께 불확실해질 수 있으므로, 상태조회 결과에 따라 terminal 수렴 또는 후속 cancel command 발행 여부를 판단해야 한다.

`M9`에서는 주문 상태를 직접 오염시킬 수 있는 이벤트 처리 리스크를 강화한다.
범위는 Kafka message envelope 중복 소비, 동일 broker event 중복 수신, ACK보다 먼저 도착한 fill, terminal 상태 이후 늦은 이벤트, cancel 처리 중 fill 도착 같은 순서 역전 시나리오다.

`M10`에서는 외부 입력 오류와 미확정 주문 탐지를 강화한다.
범위는 malformed frame/header/body 격리, pending command 응답 malformed 처리, 식별 불가능 malformed journal 기록, stale non-terminal 탐지, EOD 이후 DAY non-terminal 주문의 reconciliation 요청이다.

`M11`에서는 구현된 신뢰성 흐름을 운영자가 추적할 수 있게 만든다.
범위는 Micrometer metric, structured log, traceId 전파 누락 점검, 주문 이벤트 타임라인 조회, Gateway journal 조회, Recovery job 조회, dashboard seed, runbook drill이다.

`M12`에서는 Phase 1 릴리스 후보를 안정화한다.
범위는 요구사항/품질속성 테스트 매트릭스 점검, 전체 회귀 테스트, 성능 smoke test, flaky test 제거, 문서와 구현 차이 정리, Phase 2 backlog 확정이다.

---

## 13.4 마일스톤 상세

### M0. 프로젝트 기반

#### 목표

실제 개발을 시작할 수 있는 최소 프로젝트 골격과 로컬 실행 환경을 만든다.

#### 주요 작업

* Gradle multi-module 구성
* Java 21 / Spring Boot 기반 설정
* 서비스 모듈 생성
  * `apps:order-service`
  * `apps:broker-gateway-service`
  * `apps:recovery-service`
  * `apps:broker-simulator`
* 공통 라이브러리 모듈 생성
  * `libs:common-id`
  * `libs:common-messaging`
  * `libs:common-observability`
  * `libs:broker-protocol`
  * `libs:test-support`
* Docker Compose 기반 MySQL / Kafka 실행 환경 구성
* `local`, `test` profile 분리
* 기본 CI 구성

#### 산출물

* root Gradle 설정
* 빈 Spring Boot application 4개
* Docker Compose 파일
* 기본 CI workflow
* 로컬 실행 README 갱신

#### 완료 기준

* 전체 모듈이 clean build된다.
* 각 service app이 빈 애플리케이션으로 기동된다.
* Docker Compose로 MySQL과 Kafka가 기동된다.
* CI에서 compile과 unit test가 실행된다.

#### 검증 시나리오

* 신규 개발자가 README만 보고 로컬 인프라를 기동할 수 있다.
* 모든 모듈의 기본 test task가 성공한다.

#### 아직 하지 않는 것

* 도메인 상태머신 구현
* Kafka business message 발행/소비
* TCP 전문 구현
* 운영 dashboard 구성

---

### M1. Order 도메인 코어

#### 목표

Order Service가 주문 상태의 source of truth가 될 수 있도록 주문 상태머신, 수량 불변식, 기본 API와 DB를 구현한다.

#### 주요 작업

* `trade_order`, `order_instruction`, `order_event` migration 작성
* UUID v7 생성과 `BINARY(16)` 저장 변환 구현
* 주문 생성 API 구현
* 주문 상세 조회 API 구현
* 주문 목록 조회 API 구현
* 주문 취소 요청 API의 도메인 skeleton 구현
* `OrderStatus` 상태 전이 규칙 구현
* 수량 불변식 구현
* `PLACE`, `CANCEL` instruction 멱등성 구현
* active cancel 중복 방지
* `order_event` 상태 변경 이력 기록
* Phase 1 market state `OPEN` / `CLOSED` 최소 구현

#### 산출물

* Order domain model
* Order state transition service
* Order API controller
* Order repository / mapper
* Order Service Flyway migration
* 상태머신 unit test
* repository constraint test

#### 완료 기준

* 유효한 주문 생성 요청은 `PENDING_ACK` 주문과 `PLACE` instruction을 생성한다.
* 동일 `accountId + clientOrderId`와 동일 payload 재요청은 기존 결과를 반환한다.
* 동일 `accountId + clientOrderId`와 다른 payload 재요청은 `409 Conflict`를 반환한다.
* 취소 가능 상태에서는 `CANCEL` instruction과 `PENDING_CANCEL` 전이가 가능하다.
* 동일 `accountId + CANCEL + clientCancelRequestId`와 동일 `orderId` 및 동일 payload 재요청은 기존 취소 instruction 결과를 반환한다.
* 동일 `accountId + CANCEL + clientCancelRequestId`가 다른 `orderId` 또는 다른 payload에 재사용되면 `409 Conflict`를 반환한다.
* terminal 주문과 `UNKNOWN` 주문 취소는 거절된다.
* 상태 전이는 수량 불변식을 위반하지 않는다.
* terminal 상태 이후 잘못 도착한 이벤트는 상태를 오염시키지 않는다.

#### 검증 시나리오

* `FR-001` 주문 생성
* `FR-002` 주문 생성 멱등성
* `FR-003` 주문 조회
* `FR-004` 주문 취소 기본 규칙
* `FR-005` 주문 상태 전이 core
* `FR-006` 부분체결 수량 불변식

#### 아직 하지 않는 것

* Kafka 발행
* Broker Gateway 연동
* Broker Simulator 연동
* SSE 전송
* reconciliation workflow

---

### M2. Reliable messaging 기반

#### 목표

서비스 간 비동기 메시징의 공통 계약과 발행/소비 신뢰성 기반을 만든다.

#### 주요 작업

* 공통 message envelope 구현
* Kafka topic bootstrap 또는 topic 생성 문서화
* 서비스별 `outbox_message` migration 작성
* 서비스별 `processed_message` migration 작성
* Outbox publisher 구현
* consumer idempotency guard 구현
* message serialization / deserialization 오류 처리
* traceId 전파 규칙 구현
* consumer 실패와 parking의 최소 정책 구현

#### 산출물

* `common-messaging`
* envelope schema
* outbox publisher
* processed message guard
* Kafka topic bootstrap
* messaging integration test

#### 완료 기준

* Order Service가 주문 생성과 같은 DB transaction에서 `SubmitOrderCommand` outbox를 저장한다.
* Outbox publisher가 Kafka로 메시지를 발행하고 outbox 상태를 `SENT`로 갱신한다.
* 같은 message가 중복 소비되어도 business handler는 한 번만 실행된다.
* Kafka publish 성공 후 `SENT` 갱신 실패 상황은 중복 발행 가능성으로 모델링되고 consumer idempotency로 방어된다.
* traceId가 API request에서 outbox message와 Kafka envelope까지 전달된다.

#### 검증 시나리오

* `QA-001` 주문 접수 후 outbox publish 일시 실패
* `NFR-002` 메시지 발행 신뢰성
* `NFR-003` 메시지 소비 멱등성
* 중복 envelope 소비 시 business side effect 단일 발생

#### 아직 하지 않는 것

* Gateway TCP 송신
* Broker Simulator 실제 응답
* Recovery job orchestration

---

### M3. Broker protocol과 Simulator

#### 목표

Gateway와 Simulator가 공유하는 TCP fixed-length protocol을 구현하고, 정상/장애 시나리오를 재현할 수 있는 Broker Simulator를 만든다.

#### 주요 작업

* `broker-protocol` parser / serializer 구현
* length-prefixed frame decoder / encoder 구현
* common header parser / serializer 구현
* msgId별 body parser / serializer 구현
  * `ORDR`, `ACKN`, `RJCT`, `FILL`
  * `CXLQ`, `CXLA`, `CXLR`
  * `EXPR`, `OSTQ`, `OSTS`
* malformed 판정 구현
* Broker Simulator Netty TCP server 구현
* Simulator 주문 상태 저장소 구현
* 시나리오 주입 API 최소 구현

#### 산출물

* `libs:broker-protocol`
* parser / serializer unit test
* malformed fixture
* `apps:broker-simulator`
* simulator scenario admin API
* simulator integration test

#### 완료 기준

* 10-A의 모든 msgId가 serialize / parse round-trip 된다.
* 잘못된 frame length, body length, msgId, field value가 malformed로 분류된다.
* Simulator가 `ORDR` 요청을 받고 `ACKN` 또는 `RJCT`를 반환할 수 있다.
* Simulator가 동일 논리 이벤트를 중복 전송할 때 동일 `wireMessageId`를 재사용한다.
* Simulator가 상태조회 `OSTQ`에 대해 `OSTS`를 반환할 수 있다.

#### 검증 시나리오

* `FR-010` 외부 브로커 전문 통신
* `FR-011` Broker Simulator
* `NFR-009` 테스트 가능성

#### 아직 하지 않는 것

* Gateway DB journal 완성
* canonical broker event 전체 변환
* Order Service 상태 반영 end-to-end

---

### M4. 정상 주문 end-to-end

#### 목표

주문 생성부터 Broker Simulator ACK/Reject/Fill 수신, Gateway canonical event 변환, Order Service 상태 반영까지 정상 주문 흐름을 관통한다.

#### 주요 작업

* Broker Gateway command consumer 구현
* `SubmitOrderCommand` 처리
* Gateway DB migration 작성
  * `broker_order_binding`
  * `broker_command_attempt`
  * `broker_message_journal`
  * `outbox_message`
  * `processed_message`
* Gateway Netty TCP client 구현
* `ORDR` 송신과 `ACKN` / `RJCT` / `FILL` 수신 처리
* canonical broker event 변환
  * `BrokerOrderAcknowledged`
  * `BrokerOrderRejected`
  * `BrokerOrderPartiallyFilled`
  * `BrokerOrderFilled`
* Gateway outbox publisher로 broker event 발행
* Order Service broker event consumer 구현
* `brokerEventDedupKey` 기반 외부 사건 dedup 구현
* SSE 상태 알림 최소 구현

#### 산출물

* Gateway service core
* Gateway DB migrations
* Order broker event consumer
* 정상 주문 E2E test
* ACK / Reject / Fill integration test

#### 완료 기준

* `POST /api/orders` 호출 후 주문이 `PENDING_ACK`로 저장된다.
* `SubmitOrderCommand`가 Gateway로 전달된다.
* Gateway가 `ORDR` 전문을 송신하고 journal을 기록한다.
* Simulator `ACKN` 수신 후 Gateway가 `BrokerOrderAcknowledged`를 발행한다.
* Order Service가 주문을 `LIVE`로 전환한다.
* Simulator `RJCT` 수신 후 주문이 `REJECTED`로 종결된다.
* Simulator `FILL` 수신 후 주문이 `PARTIALLY_FILLED` 또는 `FILLED`로 수렴한다.
* `LIVE`, `REJECTED`, `PARTIALLY_FILLED`, `FILLED` 상태 변경은 `order_event`와 조회 API로 확인된다.
* M4 범위의 상태 변경 중 하나 이상은 SSE로 전달되어 SSE 연결과 event format이 동작함을 확인한다.

#### 검증 시나리오

* `UC-001` 신규 주문 생성
* `UC-005` 브로커 주문 접수/거절 반영
* `UC-006` 부분체결/완전체결 반영
* `FR-009` SSE 상태 알림의 최소 연결과 event format
* `QA-008` 주문 상태 변화 원인 추적의 기본 경로

#### 아직 하지 않는 것

* 취소 전문 송신
* cancel timeout
* reconciliation
* stale / EOD sweep

---

### M5. 기본 취소 흐름

#### 목표

`LIVE` 주문과 `PENDING_ACK` 주문에 대한 취소 요청, 취소 멱등성, Gateway 취소 전문 송신, 취소 완료/거절 반영을 구현한다.

#### 주요 작업

* `CancelOrderCommand` outbox 생성
* `PENDING_ACK` 주문의 취소 요청 접수 처리
* Gateway `CancelOrderCommand` consumer 구현
* Gateway `CXLQ` 전문 송신
* `CXLA` / `CXLR` 수신 처리
* canonical broker event 변환
  * `BrokerCancelAcknowledged`
  * `BrokerCancelRejected`
* Order Service cancel event 처리
* active cancel instruction 완료 / 거절 처리
* 취소 요청 멱등성 테스트

#### 산출물

* cancel command flow
* cancel state transition logic
* cancel idempotency test
* cancel ack/reject integration test

#### 완료 기준

* `LIVE` 주문 취소 시 `PENDING_CANCEL`로 전환되고 `CancelOrderCommand`가 발행된다.
* `PENDING_ACK` 주문 취소 시 취소 의도가 active `CANCEL` instruction으로 보존된다.
* `PENDING_ACK` 주문 취소 이후 브로커 주문 접수 정보가 확인되면 cancel command를 발행할 수 있다.
* 동일 `clientCancelRequestId` 재요청은 기존 취소 결과를 반환한다.
* active cancel이 존재할 때 다른 `clientCancelRequestId` 취소 요청은 `409 Conflict`를 반환한다.
* `CXLA` 수신 후 주문은 `CANCELED`로 종결된다.
* `CXLR` 수신 후 주문은 취소 이전의 유효 상태로 수렴한다.

#### 검증 시나리오

* `UC-007` 주문 취소 요청
* `UC-008` 중복 취소 요청 처리
* `FR-004` 주문 취소

#### 아직 하지 않는 것

* 부분체결 후 취소
* 취소 중 추가 체결
* cancel 결과 불확실성
* cancel timeout 후 reconciliation

---

### M6. 부분체결 후 취소와 경합

#### 목표

부분체결된 주문의 미체결 잔량 취소와 취소 대기 중 추가 체결 경합을 구현한다.

#### 주요 작업

* `PARTIALLY_FILLED` 주문 취소 허용
* 부분체결 후 취소 시 체결 수량 유지
* 미체결 잔량 취소 규칙 구현
* `PENDING_CANCEL` 상태에서 추가 fill 이벤트 처리
* 취소 대기 중 전량 체결 처리
* 취소 거절 시 수량 기준 상태 수렴 구현
* 부분체결 후 취소 경합 테스트 작성

#### 산출물

* partial fill cancel transition logic
* cancel race state transition test
* partial then cancel integration test

#### 완료 기준

* `PARTIALLY_FILLED` 주문에 대해 취소 요청이 가능하다.
* 취소 요청 후에도 이미 체결된 `cumQty`는 유지된다.
* 취소 완료 시 주문은 `CANCELED`로 종결되고 `leavesQty = 0`이 된다.
* 취소 요청 중 추가 부분체결이 와도 수량 불변식을 위반하지 않는다.
* 취소 요청 중 전량 체결되면 최종 상태는 `FILLED`다.
* 취소 거절 시 주문은 수량 기준으로 `LIVE`, `PARTIALLY_FILLED`, `FILLED` 중 하나로 수렴한다.

#### 검증 시나리오

* `UC-009` 부분체결 후 취소 처리
* `FR-007` 부분체결 후 취소 처리
* `QA-004` 순서 역전 이벤트 상태 수렴 일부

#### 아직 하지 않는 것

* cancel timeout
* UNKNOWN 전환
* reconciliation 기반 재취소 판단

---

### M7. Submit UNKNOWN과 Reconciliation

#### 목표

주문 요청 결과가 불확실한 경우 주문을 `UNKNOWN`으로 격리하고 상태조회 기반 reconciliation으로 수렴시킨다.

#### 주요 작업

* Gateway `ORDR` command timeout 감지
* `BrokerCommandOutcomeUnknown(commandType=SUBMIT)` 발행
* Order Service `PENDING_ACK -> UNKNOWN` 전환 처리
* `OrderReconciliationRequested` 발행
* Recovery DB migration 작성
  * `reconciliation_job`
  * `reconciliation_attempt`
  * `outbox_message`
  * `processed_message`
* Recovery lifecycle event consumer 구현
* active job 중복 방지
* `QueryOrderStatusCommand` 발행
* Gateway `OSTQ` 송신 / `OSTS` 수신 처리
* `BrokerOrderStatusSnapshot` 발행
* `StatusQueryAttemptReported` 발행
* Order Service snapshot 해석과 상태 수렴
* `OrderReconciliationResolved` / `OrderReconciliationFailed` 발행
* `NOT_FOUND` 자동 종결 금지

#### 산출물

* Recovery Service core
* reconciliation workflow
* status query command flow
* submit unknown scenario test
* `NOT_FOUND` domain resolution failure test

#### 완료 기준

* `ORDR` 응답 timeout 시 주문은 `UNKNOWN + reconciliationStatus=PENDING`으로 전환된다.
* Recovery Service는 중복 active job을 만들지 않는다.
* 상태조회 snapshot `ACCEPTED`, `PARTIAL`, `FILLED`, `CANCELED`, `REJECTED`, `EXPIRED`가 Order Service에서 올바른 주문 상태로 수렴된다.
* `STATUS_NOT_FOUND`는 자동 terminal 처리하지 않고 domain resolution failure로 기록된다.
* 상태조회 timeout은 attempt retry 정책에 따라 재시도된다.
* attempt 한도 초과 시 `ReconciliationJobFailed`가 발행되고 Order Service가 reconciliation failure를 반영한다.

#### 검증 시나리오

* `UC-011` 응답 timeout 후 UNKNOWN 처리
* `UC-012` Reconciliation으로 상태 수렴
* `FR-012` `UNKNOWN` 상태 처리
* `FR-013` Reconciliation
* `QA-005` 외부 응답 불확실성 격리

#### 아직 하지 않는 것

* cancel timeout
* active cancel 의도 유지
* stale / EOD sweep
* 모든 malformed 분기

---

### M8. Cancel UNKNOWN과 Reconciliation

#### 목표

취소 요청 결과가 불확실한 경우 주문을 `UNKNOWN`으로 격리하되 사용자의 취소 의도를 유지하고 상태조회 기반으로 수렴시킨다.

#### 주요 작업

* Gateway `CXLQ` command timeout 감지
* `BrokerCommandOutcomeUnknown(commandType=CANCEL)` 발행
* Order Service `PENDING_CANCEL -> UNKNOWN` 전환 처리
* active `CANCEL` instruction 유지
* cancel outcome unknown 기반 `OrderReconciliationRequested` 발행
* 상태조회 snapshot에 따른 상태 수렴
* snapshot이 활성 상태일 때 cancel command 재발행 판단 구현
* `PENDING_ACK` 취소 접수 후 submit 결과와 cancel 의도가 함께 남는 경우의 수렴 규칙 구현
* cancel unknown integration test 작성

#### 산출물

* cancel unknown state transition logic
* active cancel intent retention logic
* cancel reconciliation integration test

#### 완료 기준

* `CXLQ` 응답 timeout 시 주문은 `UNKNOWN + reconciliationStatus=PENDING`으로 전환된다.
* timeout만으로 Gateway가 같은 cancel command를 직접 재전송하지 않는다.
* 사용자의 active cancel 의도는 사라지지 않는다.
* 상태조회 결과가 terminal이면 해당 terminal 상태로 수렴한다.
* 상태조회 결과가 여전히 활성 상태이면 Order Service가 취소 의도에 따라 후속 cancel command 발행 여부를 결정한다.
* `PENDING_ACK` 상태에서 취소가 접수된 주문은 submit 결과 확인 후 terminal 수렴 또는 후속 cancel command 발행으로 이어진다.

#### 검증 시나리오

* `QA-002` 취소 요청 후 cancel ACK timeout
* `UC-013` 취소 의도 유지 후 자동 재시도
* `FR-004` `PENDING_ACK` 주문 취소 허용
* `FR-012` `UNKNOWN` 상태 처리
* `FR-013` Reconciliation

#### 아직 하지 않는 것

* 중복 / 순서 역전 hardening 전체
* malformed pending command response 전체
* 운영 dashboard

---

### M9. 중복과 순서 역전 hardening

#### 목표

중복 메시지, 중복 broker event, 순서 역전 event가 주문 상태와 수량을 오염시키지 않도록 고정한다.

#### 주요 작업

* message envelope 중복 소비 테스트 강화
* 동일 `brokerEventDedupKey` 중복 수신 처리
* 동일 dedup key + 다른 payload hash anomaly 처리
* ACK보다 먼저 도착한 partial fill 처리
* ACK보다 먼저 도착한 full fill 처리
* terminal 상태 이후 늦은 ACK 처리
* cancel 처리 중 fill 도착 처리
* state transition rejected 이력 기록
* duplicate / rejected transition metric 기반 추가

#### 산출물

* duplicate event scenario test
* out-of-order scenario test
* anomaly handling logic
* transition rejection audit event

#### 완료 기준

* 같은 Fill 이벤트 중복 수신 시 수량은 한 번만 반영된다.
* 같은 Kafka message 중복 소비 시 business handler는 한 번만 실행된다.
* ACK보다 Fill이 먼저 도착해도 주문은 유효한 상태로 수렴한다.
* terminal 상태 이후 늦은 이벤트가 주문 상태를 오염시키지 않는다.
* payload hash mismatch는 상태 반영 없이 anomaly로 추적된다.
* cancel 처리 중 fill이 도착해도 수량 불변식을 위반하지 않는다.

#### 검증 시나리오

* `UC-014` 중복 브로커 이벤트 처리
* `UC-015` 순서 역전 브로커 이벤트 처리
* `QA-003` 동일 외부 이벤트 단일 반영
* `QA-004` 순서 역전 상태 수렴

#### 아직 하지 않는 것

* malformed 전문 전체 격리
* stale non-terminal 탐지
* EOD reconciliation 대상 탐지

---

### M10. Malformed / Stale / EOD hardening

#### 목표

외부 입력 오류와 오래 남은 미확정 주문을 상태 오염 없이 격리하고 reconciliation 대상으로 식별한다.

#### 주요 작업

* malformed frame 처리
* malformed header 처리
* malformed body 처리
* pending command 응답 malformed 처리
* 식별 불가능 malformed journal 기록
* business semantic anomaly 처리
* `BrokerOrderExpired` 처리
* stale detector 구현
* EOD non-terminal DAY 주문 sweep 구현
* consumer 반복 실패 parking 처리

#### 산출물

* malformed fixture suite
* Gateway malformed isolation logic
* stale / EOD scheduler
* parking handler
* stale / EOD reconciliation scenario test

#### 완료 기준

* 식별 불가능 malformed 전문은 주문 상태를 직접 변경하지 않고 Gateway journal에 남는다.
* pending command 응답으로 추정되는 malformed는 필요한 경우 `BrokerCommandOutcomeUnknown` 경로로 진입한다.
* malformed metric과 journal이 남는다.
* stale non-terminal 주문은 reconciliation 대상으로 식별된다.
* EOD 이후 DAY non-terminal 주문은 임의 `EXPIRED`가 아니라 reconciliation 대상으로 식별된다.
* 반복 실패 메시지는 parking 처리되고 운영 조사 대상으로 전환된다.

#### 검증 시나리오

* `UC-016` Malformed 전문 처리
* `UC-017` Stale Order Detection and Reconciliation
* `FR-008` DAY 주문 만료 처리
* `QA-006` 미확정 non-terminal 주문 복구 대상 식별
* `QA-011` 장애 시나리오 재현 가능성

#### 아직 하지 않는 것

* 운영 콘솔 UI
* parking message 재주입 도구
* 장기간 soak test

---

### M11. 관측성과 운영 준비

#### 목표

구현된 주문 신뢰성 흐름을 운영자가 추적할 수 있도록 metric, structured log, trace field, 운영 조회, SSE 전달 검증, runbook을 준비한다.

#### 주요 작업

* Micrometer / Actuator 적용
* Order Service metrics 구현
* Gateway metrics 구현
* Recovery metrics 구현
* Outbox / consumer metrics 구현
* 구조화 로그 필드 표준화
* traceId 전파 누락 점검
* 전체 Phase 1 상태 변경에 대한 SSE 발행 범위 점검
* 운영 조회 API 구현 후보
  * 주문 이벤트 타임라인 조회
  * Gateway command attempt 조회
  * Gateway message journal 조회
  * Recovery job / attempt 조회
* Prometheus scrape 설정
* Grafana dashboard seed 작성
* runbook drill 실행

#### 산출물

* metrics implementation
* structured logging policy
* dashboard json 또는 dashboard 구성 문서
* operational query endpoints
* runbook drill result

#### 완료 기준

* `orderId`, `wireMessageId`, `traceId`, `jobId`, `attemptId` 기준으로 주요 흐름을 추적할 수 있다.
* `UNKNOWN` 진입, reconciliation 성공/실패, malformed, outbox backlog, consumer failure metric이 수집된다.
* 부분체결, 완전체결, 취소, 거절, 만료, `UNKNOWN` 진입 이벤트가 SSE로 전달된다.
* SSE 연결이 끊겨도 주문 조회 API로 최종 상태를 확인할 수 있다.
* dashboard에서 상태별 주문 수, outbox 상태, Gateway timeout, Recovery job 상태를 확인할 수 있다.
* runbook 절차대로 특정 장애 시나리오의 원인을 찾아갈 수 있다.

#### 검증 시나리오

* `UC-018` 주문 장애 추적
* `FR-009` 실시간 주문 상태 알림
* `FR-014` 운영 추적 정보 제공
* `QA-008` 주문 상태 변화 원인 추적
* `QA-009` 상태 변화 반영 지연 관측

#### 아직 하지 않는 것

* 운영 콘솔 UI
* 알림 채널 연동
* 장기 보존 정책 자동화

---

### M12. Phase 1 안정화

#### 목표

Phase 1 범위의 기능, 신뢰성 시나리오, 운영 추적, 문서를 하나의 릴리스 후보로 정리한다.

#### 주요 작업

* 요구사항 기반 테스트 매트릭스 점검
* 품질 속성 기반 테스트 매트릭스 점검
* 전체 회귀 테스트 실행
* reliability scenario 반복 실행
* performance smoke test 실행
* Testcontainers 기반 통합 테스트 안정화
* flaky test 제거
* README 실행 절차 갱신
* docs 01-13과 구현 차이 정리
* Phase 2 backlog 정리

#### 산출물

* Phase 1 test report
* performance smoke result
* final README
* docs implementation notes
* Phase 2 backlog

#### 완료 기준

* `Must Have` 요구사항이 자동화 테스트 또는 명시적 수동 검증 절차로 확인된다.
* Phase 1에 포함하기로 한 `Should Have` 항목이 완료되거나 Phase 2로 이동된다.
* 정상 주문, 취소, 부분체결, timeout, reconciliation, 중복, 순서 역전, malformed, stale, EOD 시나리오가 재현 가능하다.
* 로컬 환경에서 처음부터 실행해 주요 시나리오를 검증할 수 있다.
* 문서와 구현 사이의 알려진 차이가 별도 목록으로 정리되어 있다.

#### 검증 시나리오

* `12.6.1` 요구사항 기반 매트릭스 전체 점검
* `12.6.2` 품질 속성 기반 매트릭스 전체 점검
* `12.18` 배포 전 점검 체크리스트

#### 아직 하지 않는 것

* 멀티 브로커 라우팅
* fallback
* 실제 브로커 연동
* 운영 콘솔 UI
* 장기 soak test

---

## 13.5 Phase 1 완료 기준

Phase 1은 모든 마일스톤이 끝났다는 사실보다, 다음 기준을 만족하는지로 완료 여부를 판단한다.

### 기능 기준

* 주문 생성, 조회, 취소 API가 동작한다.
* 주문 생성 요청과 취소 요청은 멱등하게 처리된다.
* 주문 접수, 거절, 부분체결, 완전체결, 취소, 만료 상태가 반영된다.
* 부분체결 후 취소와 취소 중 추가 체결이 수량 불변식을 위반하지 않는다.
* 주문 상태 변경은 SSE로 전달되며, SSE 연결이 끊겨도 조회 API로 최종 주문 상태를 확인할 수 있다.
* Broker Gateway가 TCP 전문을 송수신하고 canonical broker event로 변환한다.
* Broker Simulator가 정상/장애 시나리오를 재현한다.
* `UNKNOWN` 주문이 reconciliation으로 최종 상태 또는 조사 가능한 실패 상태로 수렴한다.

### 신뢰성 기준

* DB 상태 변경과 메시지 발행 사이의 유실 위험은 Outbox로 방어된다.
* 중복 message envelope는 processed message로 방어된다.
* 중복 broker event는 `brokerEventDedupKey`로 방어된다.
* 순서 역전 이벤트가 terminal 상태나 수량 불변식을 오염시키지 않는다.
* submit/cancel 결과 불확실성은 임의 성공/실패가 아니라 `UNKNOWN`과 reconciliation으로 처리된다.
* malformed 전문은 Gateway에서 격리되고 주문 상태를 직접 변경하지 않는다.
* stale / EOD non-terminal 주문은 reconciliation 대상으로 식별된다.

### 운영 기준

* 특정 `orderId`의 상태 변경 원인을 추적할 수 있다.
* `wireMessageId` 기준으로 브로커 전문 송수신 이력을 찾을 수 있다.
* `traceId` 기준으로 API, message, Gateway, Recovery 흐름을 연결할 수 있다.
* reconciliation job과 attempt 이력을 확인할 수 있다.
* `UNKNOWN`, malformed, outbox backlog, consumer failure, reconciliation failure를 metric으로 관측할 수 있다.
* 최소 runbook으로 주요 장애 시나리오를 조사할 수 있다.

### 테스트 기준

* 요구사항 기반 테스트 매트릭스의 Must Have 항목이 검증된다.
* 품질 속성 기반 테스트 매트릭스의 Must 항목이 검증된다.
* 정상 흐름뿐 아니라 timeout, duplicate, out-of-order, malformed, stale, EOD 시나리오가 재현 가능하다.
* performance smoke test로 초기 벤치마크 지표를 측정한다.
* 로컬 환경에서 처음부터 주요 시나리오를 실행할 수 있다.
