# 8. ADR / DDR

## 8.1 목적

이 문서는 프로젝트의 주요 아키텍처 결정과 상세 설계 결정을 기록한다.

목적은 다음이다.

1. 왜 현재 구조를 선택했는지 설명한다.
2. 선택하지 않은 대안을 명시한다.
3. 각 결정이 어떤 품질 속성 시나리오를 만족하기 위한 것인지 연결한다.
4. 이후 구현·테스트·문서화 과정에서 설계 기준이 흔들리지 않도록 한다.

---

## 8.2 ADR / DDR 구분

| 구분  | 의미                           | 예시                                                    |
| --- | ---------------------------- | ----------------------------------------------------- |
| ADR | Architecture Decision Record | 서비스 분리, 메시징 방식, 복구 구조, 브로커 연계 경계                      |
| DDR | Design Decision Record       | 주문 상태 모델, instruction 모델, 식별자 규칙, 중복 이벤트 판정, 상태 전이 규칙 |

### 기준

* 시스템 구조, 배포 단위, 주요 기술 선택에 영향을 주면 **ADR**
* 특정 도메인 모델, 상태 전이, 프로토콜 규칙, 이벤트 처리 규칙이면 **DDR**

---

# 8.3 ADR 목록

| ID        | 제목                                                                  | 상태       |
| --------- | ------------------------------------------------------------------- | -------- |
| `ADR-001` | Order / Broker Gateway / Recovery 3개 서비스로 분리한다                      | Accepted |
| `ADR-002` | Order Service를 주문 상태와 OrderInstruction 상태의 단일 소유자로 둔다               | Accepted |
| `ADR-003` | 서비스 간 상호작용은 비동기 메시징 중심으로 구성한다                                       | Accepted |
| `ADR-004` | 메시지 브로커로 Kafka를 사용한다                                                | Accepted |
| `ADR-005` | 메시지 발행 신뢰성은 Transactional Outbox로 보강한다                              | Accepted |
| `ADR-006` | 메시지 소비 중복은 processed message 기록으로 방어한다                              | Accepted |
| `ADR-007` | 외부 브로커 통신과 브로커 식별자는 Broker Gateway에 격리하고 canonical event로 변환한다      | Accepted |
| `ADR-008` | 불확실한 주문 상태는 `UNKNOWN`으로 격리하고 Recovery Service가 reconciliation을 수행한다 | Accepted |
| `ADR-009` | Broker Simulator는 Netty 기반 TCP fixed-length message server로 구현한다    | Accepted |
| `ADR-010` | SSE 기반 주문 상태 알림은 Order Service에서 제공한다                               | Accepted |

---

# 8.4 DDR 목록

| ID        | 제목                                                             | 상태       |
| --------- | -------------------------------------------------------------- | -------- |
| `DDR-001` | 주문 상태 모델에 `UNKNOWN`, `EXPIRED`, `PENDING_CANCEL`을 포함한다         | Accepted |
| `DDR-002` | `reconciliationStatus`는 주문 상태와 분리한다                            | Accepted |
| `DDR-003` | 체결 모델은 가격 없이 수량 중심으로 단순화한다                                     | Accepted |
| `DDR-004` | 사용자 주문 관련 요청은 `OrderInstruction`으로 모델링한다                       | Accepted |
| `DDR-005` | 식별자는 생성 주체와 책임 경계에 따라 분리한다                                     | Accepted |
| `DDR-006` | 브로커 이벤트 중복 판단은 Gateway가 부여한 opaque `brokerEventDedupKey`로 수행한다 | Accepted |
| `DDR-007` | Order 도메인 모델은 브로커 식별자를 소유하지 않는다                                | Accepted |
| `DDR-008` | Phase 1 브로커 전문은 `orderId`를 주문 참조값으로 포함한다                       | Accepted |
| `DDR-009` | 부분체결 후 취소는 체결분 확정 + 미체결 잔량 취소로 처리한다                            | Accepted |
| `DDR-010` | 식별 불가능한 malformed 전문은 주문 상태를 직접 변경하지 않는다                       | Accepted |
| `DDR-011` | `NOT_FOUND` reconciliation 결과는 자동 종결하지 않는다                     | Accepted |

---

# 8.5 ADR 상세

## ADR-001. Order / Broker Gateway / Recovery 3개 서비스로 분리한다

### Status

Accepted

### Context

시스템은 다음 책임을 동시에 가져야 한다.

* 사용자 주문 생성/조회/취소
* 사용자 주문 관련 instruction 처리
* 주문 상태머신과 수량 정합성 관리
* 외부 브로커 TCP 전문 통신
* 브로커 응답 지연/유실/중복/순서 역전 처리
* `UNKNOWN` 주문 복구
* stale non-terminal 주문 탐지

이 책임을 하나의 서비스에 모두 넣으면 브로커 통신, 주문 상태 변경, instruction 처리, 복구 정책이 강하게 결합된다.

### Options

| 대안          | 설명                                                          | 평가                  |
| ----------- | ----------------------------------------------------------- | ------------------- |
| 단일 모놀리식 서비스 | 모든 기능을 하나의 Spring Boot 애플리케이션에 구현                           | 구현은 빠르지만 책임 경계가 흐려짐 |
| 3개 서비스 분리   | Order Service, Broker Gateway Service, Recovery Service로 분리 | 책임 경계와 장애 격리가 명확함   |
| 풀 MSA       | 주문, instruction, 알림, 복구, 브로커, 운영 기능을 더 세분화                  | 개인 프로젝트 범위에서 과도함    |

### Decision

Phase 1에서는 다음 3개 서비스로 분리한다.

* **Order Service**
* **Broker Gateway Service**
* **Recovery Service**

### Consequences

장점:

* 주문 상태 변경 책임이 명확해진다.
* 사용자 instruction 처리와 주문 상태 변경을 Order Service에 집중시킬 수 있다.
* 외부 브로커 통신 장애를 사용자 API와 분리할 수 있다.
* 복구 로직을 별도 흐름으로 설계할 수 있다.
* Phase 2의 멀티 브로커 확장 지점이 선명해진다.

단점:

* 메시징, 장애 처리, 로컬 개발 환경 구성이 필요하다.
* 통합 테스트 복잡도가 증가한다.

### Related Quality Attributes

* Reliability
* Recoverability
* Fault Isolation
* Modifiability

---

## ADR-002. Order Service를 주문 상태와 OrderInstruction 상태의 단일 소유자로 둔다

### Status

Accepted

### Context

브로커 이벤트, 사용자 instruction, reconciliation 결과, 만료 이벤트 등 여러 입력이 주문 상태를 변경할 수 있다.
또한 사용자의 주문 생성/취소 instruction은 접수, 진행, 완료, 거절, 실패 상태를 가진다.

여러 서비스가 주문 상태나 instruction 상태를 직접 변경하면 상태 경쟁과 정합성 문제가 발생한다.

### Options

| 대안                             | 설명                                                | 평가                      |
| ------------------------------ | ------------------------------------------------- | ----------------------- |
| 각 서비스가 직접 주문/instruction 상태 변경 | Gateway, Recovery가 직접 상태 갱신                       | 빠르지만 상태 변경 주체가 분산됨      |
| Order Service만 상태 변경           | 외부 사건은 event/command로 전달하고 Order Service가 상태머신 적용 | 일관성 높음                  |
| 이벤트 소싱 전면 적용                   | 모든 상태를 이벤트 재생으로 구성                                | 학습 효과는 크지만 Phase 1에는 과함 |

### Decision

주문 상태 변경과 `OrderInstruction` 상태 변경은 **Order Service만 수행**한다.

Broker Gateway와 Recovery Service는 주문 상태나 instruction 상태를 직접 변경하지 않는다.
이들은 외부 사건, command outcome, reconciliation result를 전달하고, Order Service가 상태머신과 도메인 규칙을 통해 최종 상태를 결정한다.

### Consequences

장점:

* 상태 전이 규칙이 한 곳에 모인다.
* 수량 불변식 검증이 일관된다.
* instruction 상태와 주문 상태의 조합 규칙을 한 곳에서 통제할 수 있다.
* 중복/순서 역전 이벤트 처리 기준이 명확해진다.

단점:

* 모든 외부 사건이 Order Service로 전달되어야 한다.
* Order Service의 상태머신 품질이 매우 중요해진다.

### Related Quality Attributes

* Consistency
* Observability
* Recoverability

---

## ADR-003. 서비스 간 상호작용은 비동기 메시징 중심으로 구성한다

### Status

Accepted

### Context

사용자 주문 생성 API가 외부 브로커 응답을 동기적으로 기다리면 브로커 장애가 사용자 API 장애로 전파된다.
또한 브로커 응답은 비동기적으로 발생하는 체결/취소/만료 이벤트를 포함한다.

### Options

| 대안            | 설명                                    | 평가               |
| ------------- | ------------------------------------- | ---------------- |
| 동기 REST 호출 중심 | Order Service가 Gateway를 동기 호출하고 결과 대기 | 단순하지만 브로커 장애에 취약 |
| 비동기 메시징 중심    | instruction 접수와 브로커 처리 결과 반영을 분리      | 장애 격리와 확장성에 유리   |
| 혼합 방식         | 일부는 동기, 일부는 비동기                       | 경계가 애매해질 위험      |

### Decision

서비스 간 핵심 흐름은 비동기 메시징 중심으로 구성한다.

주요 비동기 흐름:

* Submit Order Command
* Cancel Order Command
* Query Order Status Command
* Broker Event
* Order Lifecycle Event

### Consequences

장점:

* 사용자 API가 외부 브로커 응답에 동기 의존하지 않는다.
* 브로커 장애 격리가 쉬워진다.
* 재처리와 복구 흐름을 설계하기 좋다.
* 주문 생성/취소 instruction 접수와 브로커 결과 반영을 분리할 수 있다.

단점:

* 메시지 중복, 순서, 발행 실패 처리가 필요하다.
* 최종 일관성 모델을 명확히 설명해야 한다.

### Related Quality Attributes

* Reliability
* Fault Isolation
* Recoverability
* Consistency

---

## ADR-004. 메시지 브로커로 Kafka를 사용한다

### Status

Accepted

### Context

서비스 간 명령과 이벤트를 비동기로 전달해야 한다.
이벤트 흐름은 주문 단위 순서, 재처리, 관측성, 향후 확장성을 고려해야 한다.

### Options

| 대안              | 설명                               | 평가                            |
| --------------- | -------------------------------- | ----------------------------- |
| In-memory queue | 구현이 단순함                          | 서비스 분리와 장애 복구 설명이 약함          |
| Redis Streams   | 비교적 가볍고 구현 쉬움                    | Kafka 대비 이벤트 로그/파티션 모델 설명이 약함 |
| RabbitMQ        | command queue에 적합                | 이벤트 스트림/순서/로그 관점은 Kafka가 더 적합 |
| Kafka           | 주문별 key 기반 순서, 이벤트 로그, 재처리 모델 제공 | 로컬 구성 복잡도 증가                  |

### Decision

Phase 1의 메시지 브로커로 **Kafka**를 사용한다.

주문 관련 메시지는 기본적으로 `orderId`를 key로 사용한다.

### Consequences

장점:

* 주문 단위 메시지 ordering을 설명하기 좋다.
* at-least-once delivery와 idempotent consumer 설계를 명확히 보여줄 수 있다.
* 향후 운영/모니터링/lag 측정 포인트가 생긴다.

단점:

* 로컬 개발 환경이 복잡해진다.
* 메시지 중복 처리와 offset 처리에 대한 테스트가 필요하다.

### Related Quality Attributes

* Reliability
* Consistency
* Observability
* Testability

---

## ADR-005. 메시지 발행 신뢰성은 Transactional Outbox로 보강한다

### Status

Accepted

### Context

서비스는 instruction 접수, 주문 상태 변경, 복구 상태 변경과 함께 비동기 메시지를 발행해야 한다.
상태 변경은 성공했지만 메시지 발행 전에 장애가 발생하면, 정상 접수된 instruction이나 상태 변경 이벤트가 유실될 수 있다.

### Options

| 대안                       | 설명                               | 평가                            |
| ------------------------ | -------------------------------- | ----------------------------- |
| 상태 변경 후 즉시 Kafka publish | 구현 단순                            | 상태 변경 성공 후 publish 실패 시 유실 위험 |
| Kafka transaction 사용     | 일부 원자성 보장                        | DB 트랜잭션과 묶기 어렵고 복잡함           |
| Transactional Outbox     | 상태 변경과 발행 대상 메시지를 같은 로컬 트랜잭션에 저장 | 현실적이고 설명 가능                   |
| CDC 기반 Outbox            | DB 변경 로그 기반 발행                   | 운영 구성이 무거움                    |

### Decision

메시지 발행 신뢰성은 **Transactional Outbox** 패턴으로 보강한다.

적용 대상:

* Order Service
* Broker Gateway Service
* Recovery Service

### Consequences

장점:

* 상태 변경과 발행 대상 메시지를 같은 트랜잭션에 묶을 수 있다.
* producer 장애 후에도 미발행 메시지를 재처리할 수 있다.
* 분산 트랜잭션 없이 신뢰성을 보강할 수 있다.

단점:

* outbox publisher 구현이 필요하다.
* 중복 발행 가능성을 consumer 쪽에서 방어해야 한다.
* outbox 테이블 관리와 재시도 정책이 필요하다.

### Related Quality Attributes

* Reliability
* Recoverability
* Observability

---

## ADR-006. 메시지 소비 중복은 processed message 기록으로 방어한다

### Status

Accepted

### Context

Kafka 기반 at-least-once 처리에서는 같은 message envelope가 중복 소비될 수 있다.
중복 메시지가 비즈니스 처리까지 반복되면 주문 상태 변경, command 전송, reconciliation job 생성 등이 중복 실행될 수 있다.

단, processed message 기록은 **같은 message envelope의 재소비**를 막기 위한 장치다.
서로 다른 message envelope에 담긴 같은 외부 브로커 사건의 중복 반영은 별도의 broker event semantic dedup 규칙으로 방어한다.

### Options

| 대안                     | 설명                   | 평가                           |
| ---------------------- | -------------------- | ---------------------------- |
| consumer 로직을 단순 재실행    | 구현 단순                | 중복 상태 변경 위험                  |
| aggregate version만 사용  | 일부 경쟁 조건 방어          | 메시지 중복 여부 추적이 약함             |
| processed message 기록   | consumer별 처리 메시지를 기록 | 명확하고 테스트 가능                  |
| semantic dedup key만 사용 | 논리 이벤트 중복 방어         | message envelope 재소비 방어에는 부족 |

### Decision

각 consumer는 **processed message 기록**을 통해 동일 message envelope의 중복 소비를 방어한다.

브로커 이벤트는 추가로 Broker Gateway가 부여한 opaque `brokerEventDedupKey`를 사용해 동일 외부 사건의 중복 반영을 방어한다.

### Consequences

장점:

* 동일 메시지 재소비를 방어할 수 있다.
* broker event의 논리적 중복도 별도 규칙으로 방어할 수 있다.
* 장애 후 재처리 테스트가 명확해진다.

단점:

* processed message 저장소가 필요하다.
* 오래된 처리 기록의 보관/정리 정책이 필요하다.
* message envelope 중복과 외부 사건 중복을 구분해서 문서화해야 한다.

### Related Quality Attributes

* Consistency
* Reliability
* Testability

---

## ADR-007. 외부 브로커 통신과 브로커 식별자는 Broker Gateway에 격리하고 canonical event로 변환한다

### Status

Accepted

### Context

외부 브로커 통신은 TCP 전문, 고정 길이 body, padding, malformed 처리, 브로커 주문 ID 등 주문 도메인 모델과 다른 관심사를 가진다.
이 전문 포맷과 브로커 식별자가 Order Service로 새어 나오면 향후 브로커 추가/교체가 어려워진다.

### Options

| 대안                                   | 설명        | 평가           |
| ------------------------------------ | --------- | ------------ |
| Order Service가 전문 직접 처리              | 빠르게 구현 가능 | 도메인과 프로토콜 결합 |
| Gateway에서 전문 처리 후 canonical event 변환 | 경계 명확     | 변환 계층 필요     |
| 브로커별 별도 서비스                          | 확장성은 좋음   | Phase 1에는 과함 |

### Decision

외부 브로커 통신은 Broker Gateway에 격리한다.

Broker Gateway는 다음을 소유한다.

* 브로커 선택
* 브로커 주문 식별자
* 브로커 전문 송수신
* 전문 parsing / serialization
* malformed 전문 처리
* command attempt
* broker order binding
* canonical broker event 생성
* canonical event의 `orderId`와 `brokerEventDedupKey` 부여

Order Service는 브로커 코드, 브로커 주문 ID, 전문 ID 구조를 주문 도메인 상태 판단에 사용하지 않는다.
Order Service는 Broker Gateway가 발행한 canonical broker event만 처리한다.

### Consequences

장점:

* Order Service는 브로커 전문 포맷과 브로커 식별자 구조를 모른다.
* 브로커 추가/교체 시 Gateway 쪽 변경으로 제한할 수 있다.
* malformed 전문 처리와 journal 기록 위치가 명확해진다.
* 브로커 주문 ID와 전문 송수신 이력의 소유권이 명확해진다.
* Order Service는 `orderId`와 canonical event만으로 상태머신을 유지할 수 있다.

단점:

* Gateway 변환 로직과 테스트가 필요하다.
* canonical event schema를 신중히 정의해야 한다.
* 운영 콘솔에서 주문 상태와 브로커 전문 이력을 함께 보여주려면 cross-service 조회 또는 read model이 필요할 수 있다.

### Related Quality Attributes

* Modifiability
* Observability
* Testability
* Consistency

---

## ADR-008. 불확실한 주문 상태는 `UNKNOWN`으로 격리하고 Recovery Service가 reconciliation을 수행한다

### Status

Accepted

### Context

외부 브로커 응답이 timeout되거나 malformed로 처리되어 결과를 알 수 없는 경우, 시스템은 주문 성공/실패 또는 취소 성공/실패를 단정할 수 없다.
이 상태에서 자동 재전송하거나 실패 처리하면 중복 주문, 잘못된 취소, 상태 오염이 발생할 수 있다.

### Options

| 대안                  | 설명       | 평가                       |
| ------------------- | -------- | ------------------------ |
| timeout 시 실패 처리     | 단순       | 실제 브로커에 주문이 들어갔을 가능성을 무시 |
| timeout 시 즉시 재전송    | 빠른 복구 가능 | 중복 주문/취소 위험              |
| `UNKNOWN` 격리 후 상태조회 | 보수적이고 안전 | 복구 흐름 필요                 |

### Decision

외부 결과가 불확실하면 주문을 `UNKNOWN`으로 격리한다.
Recovery Service가 상태조회 기반 reconciliation을 수행한다.

### Consequences

장점:

* 성공/실패를 잘못 단정하지 않는다.
* 브로커 상태와 내부 상태를 수렴시킬 수 있다.
* 운영자가 확인 가능한 복구 이력이 남는다.
* active instruction이 있으면 reconciliation 결과에 따라 instruction을 유지하거나 종료할 수 있다.

단점:

* 사용자는 일시적으로 “확인 중” 상태를 보게 된다.
* Recovery Service와 상태조회 흐름이 필요하다.
* `NOT_FOUND` 등 미해결 결과 정책이 필요하다.

### Related Quality Attributes

* Recoverability
* Reliability
* Fault Isolation
* Operability

---

## ADR-009. Broker Simulator는 Netty 기반 TCP fixed-length message server로 구현한다

### Status

Accepted

### Context

프로젝트는 실제 브로커 연동을 하지 않는다.
하지만 단순 REST stub은 대외기관 연계의 핵심 문제인 framing, 전문 파싱, malformed, 지연, 중복, 순서 역전을 재현하기 어렵다.

### Options

| 대안                               | 설명                | 평가                  |
| -------------------------------- | ----------------- | ------------------- |
| REST stub                        | 구현 가장 쉬움          | 대외 전문 통신 학습 효과 부족   |
| Kafka 기반 mock                    | 비동기 이벤트 재현 쉬움     | 실제 브로커 TCP 연계 느낌 약함 |
| Netty TCP fixed-length simulator | 전문 통신 핵심 구조 재현 가능 | 구현 공수 증가            |
| FIX 구현                           | 현실감 높음            | Phase 1 범위 초과       |

### Decision

Broker Simulator는 **Netty 기반 TCP fixed-length message server**로 구현한다.

단, 범위는 다음으로 제한한다.

* length-prefixed frame
* common header
* fixed-length body
* 전문 ID별 parser/serializer
* field padding
* numeric zero padding
* malformed 전문 처리
* scenario injection

### Consequences

장점:

* 금융권 전문 통신의 핵심 구조를 단순화해 보여줄 수 있다.
* Gateway의 protocol isolation 설계를 검증할 수 있다.
* 장애/중복/순서 역전 시나리오를 재현할 수 있다.

단점:

* Netty와 전문 parser 구현 공수가 필요하다.
* 프로토콜 구현에 과도하게 빠지지 않도록 범위 통제가 필요하다.

### Related Quality Attributes

* Testability
* Modifiability
* Observability

---

## ADR-010. SSE 기반 주문 상태 알림은 Order Service에서 제공한다

### Status

Accepted

### Context

사용자는 주문 상태 변경을 실시간으로 확인해야 한다.
Phase 1에서는 복잡한 notification service를 별도로 두기보다, Order Service가 상태 변경 직후 SSE를 통해 알림을 제공하는 구조가 단순하다.

### Options

| 대안                      | 설명                  | 평가                          |
| ----------------------- | ------------------- | --------------------------- |
| 클라이언트 polling           | 구현 쉬움               | 실시간성 약함, 부하 증가              |
| Order Service 내 SSE     | 단순하고 상태 변경 직후 전달 가능 | 연결 관리 책임이 Order Service에 있음 |
| 별도 Notification Service | 확장성 좋음              | Phase 1에는 과함                |
| WebSocket               | 양방향 가능              | 현재 요구에는 과함                  |

### Decision

Phase 1에서는 **Order Service가 SSE 기반 주문 상태 알림을 직접 제공**한다.

### Consequences

장점:

* 구현이 단순하다.
* 주문 상태 변경과 알림 발행의 경계가 명확하다.
* 사용자-facing 제품성이 살아난다.

단점:

* SSE 연결 관리가 Order Service 책임이 된다.
* 대규모 연결 확장은 Phase 1 범위 밖이다.

### Related Quality Attributes

* Timeliness
* Simplicity

---

# 8.6 DDR 상세

## DDR-001. 주문 상태 모델에 `UNKNOWN`, `EXPIRED`, `PENDING_CANCEL`을 포함한다

### Status

Accepted

### Context

외부 브로커 응답은 지연, 유실, 순서 역전될 수 있다.
DAY 주문은 장 마감 시 미체결 잔량이 만료될 수 있다.
취소 instruction은 즉시 완료되지 않고 브로커 결과를 기다려야 한다.

### Decision

OrderStatus에 다음 상태를 포함한다.

* `PENDING_ACK`
* `LIVE`
* `PARTIALLY_FILLED`
* `PENDING_CANCEL`
* `UNKNOWN`
* `FILLED`
* `CANCELED`
* `REJECTED`
* `EXPIRED`

### Consequences

장점:

* 불확실한 상태를 실패로 단정하지 않는다.
* 취소 대기 중 추가 체결을 표현할 수 있다.
* DAY 주문 만료를 명확히 표현할 수 있다.

단점:

* 상태 전이 규칙이 복잡해진다.
* 테스트해야 할 상태 조합이 늘어난다.

---

## DDR-002. `reconciliationStatus`는 주문 상태와 분리한다

### Status

Accepted

### Context

`RECONCILING`을 주문 상태로 넣으면 비즈니스 상태와 복구 작업 상태가 섞인다.

### Decision

주문 상태와 별도로 `reconciliationStatus`를 둔다.

값:

* `NONE`
* `PENDING`
* `RUNNING`
* `RESOLVED`
* `FAILED`

### Consequences

장점:

* 주문의 비즈니스 상태와 복구 작업 상태를 분리할 수 있다.
* `UNKNOWN + PENDING`, `UNKNOWN + FAILED` 같은 조합이 가능하다.

단점:

* 주문 상태와 reconciliation 상태 간 정합성 규칙이 필요하다.

---

## DDR-003. 체결 모델은 가격 없이 수량 중심으로 단순화한다

### Status

Accepted

### Context

프로젝트의 핵심은 가격 평가, 손익, 정산이 아니라 주문 상태와 수량 정합성이다.
실제 지정가 주문에서도 체결가는 존재하지만, Phase 1 범위에서는 체결 가격을 다루지 않는다.

### Decision

Phase 1의 체결 이벤트는 다음 수량만 가진다.

* `lastFillQty`
* `cumQty`
* `leavesQty`

`lastFillPrice`, `avgFillPrice`는 제외한다.

### Consequences

장점:

* 프로젝트 범위가 주문 상태 추적에 집중된다.
* 포지션/손익/정산 도메인으로 번지는 것을 방지한다.

단점:

* 실제 매매 시스템의 체결 정보보다 단순하다.
* 후속으로 포지션/손익을 추가하려면 체결가 모델이 필요하다.

---

## DDR-004. 사용자 주문 관련 요청은 `OrderInstruction`으로 모델링한다

### Status

Accepted

### Context

기존 설계에서는 취소 요청을 별도 `CancelRequest`로 모델링했다. 그러나 취소만 전용 모델로 두면 이벤트나 사용자 action이 늘어날 때마다 별도 모델이 추가되는 구조처럼 보인다.

또한 주문 생성 요청과 취소 요청은 모두 클라이언트가 생성한 멱등성 키를 가지고, 시스템 내부에서 접수/진행/완료/실패 상태를 가진다.

### Decision

사용자의 주문 관련 요청은 `OrderInstruction`으로 일반화한다.

Phase 1에서 지원하는 instruction type은 다음과 같다.

* `PLACE`: 신규 주문 생성
* `CANCEL`: 미체결 잔량 취소

향후 주문 정정, 수동 복구, 운영자 개입 등 장기 실행 요청이 필요해지면 instruction type을 추가해 확장할 수 있다.

API 레벨에서는 사용자 의미에 맞게 `clientOrderId`, `clientCancelRequestId`를 사용한다.
내부 모델에서는 이를 `clientInstructionId`로 일반화한다.

### Consequences

장점:

* 주문 생성과 취소 요청의 멱등성 모델이 일관된다.
* 취소 전용 모델이 사라지고 instruction type 확장 구조가 생긴다.
* instruction의 현재 처리 상태와 이벤트 이력을 구분할 수 있다.

단점:

* `CancelRequest`보다 `OrderInstruction`은 추상적이므로 문서화가 필요하다.
* instruction type별 상태 의미를 명확히 정의해야 한다.

---

## DDR-005. 식별자는 생성 주체와 책임 경계에 따라 분리한다

### Status

Accepted

### Context

시스템에는 주문, 사용자 instruction, 클라이언트 멱등성 키, 브로커 전문, 브로커 주문 ID, 추적 ID가 함께 등장한다.
이들을 하나의 “주문 ID”처럼 취급하면 멱등성, 전문 추적, 브로커 연계, 도메인 상태 판단의 책임 경계가 흐려진다.

### Decision

다음 식별자를 구분한다.

| 식별자                     | 목적                               |
| ----------------------- | -------------------------------- |
| `orderId`               | 내부 주문 aggregate 식별               |
| `instructionId`         | 내부 instruction 식별                |
| `clientOrderId`         | 주문 생성 instruction의 클라이언트 멱등성 키   |
| `clientCancelRequestId` | 취소 instruction의 클라이언트 멱등성 키      |
| `wireMessageId`         | TCP 전문 단위 식별 및 요청/응답 correlation |
| `brokerEventDedupKey`   | 동일 외부 브로커 사건의 중복 반영 방지           |
| `traceId`               | end-to-end 관측성 추적                |
| `brokerOrderId`         | 브로커 측 주문 식별자. Broker Gateway가 소유 |

### Consequences

장점:

* 멱등성, 내부 식별, 전문 추적, 관측성, 브로커 식별이 분리된다.
* Order Service가 브로커 식별자를 상태 판단에 사용하지 않도록 경계를 명확히 할 수 있다.
* 장애 분석 시 각 식별자의 역할이 선명하다.

단점:

* 식별자 수가 많아 문서화가 필요하다.
* API 이름과 내부 모델 이름 사이의 매핑을 명확히 유지해야 한다.

---

## DDR-006. 브로커 이벤트 중복 판단은 Gateway가 부여한 opaque `brokerEventDedupKey`로 수행한다

### Status

Accepted

### Context

동일한 브로커 이벤트가 중복 수신될 수 있다.
중복 체결 이벤트를 다시 반영하면 주문 수량이 오염된다.

기존에는 중복 판단 키를 `brokerCode + msgId + wireMessageId`로 정의했다. 이 방식 자체는 유효하지만, 이를 Order Service가 이해하는 구조로 두면 브로커 통신 관심사가 Order 도메인으로 새어 들어온다.

### Decision

Broker Gateway가 canonical broker event를 생성할 때 `brokerEventDedupKey`를 부여한다.

Order Service는 `brokerEventDedupKey`를 opaque value로 취급한다.
즉, 내부 구조를 파싱하지 않고 동일성 비교에만 사용한다.

Phase 1에서 Broker Gateway는 내부적으로 다음 조합으로 key를 생성할 수 있다.

```text id="q243hg"
brokerCode + ":" + msgId + ":" + wireMessageId
```

하지만 이 조합 방식은 Gateway 내부 구현 규칙이다.

### Consequences

장점:

* Order Service는 브로커 코드, 전문 ID, wire message 구조를 알 필요가 없다.
* 브로커 추가/교체 시 dedup key 생성 방식 변경을 Gateway 내부로 제한할 수 있다.
* 같은 외부 사건의 중복 반영을 방어할 수 있다.

단점:

* Gateway가 같은 외부 사건에는 항상 같은 `brokerEventDedupKey`를 부여해야 한다.
* 같은 key에 다른 payload가 들어오는 anomaly 처리 정책이 필요하다.

---

## DDR-007. Order 도메인 모델은 브로커 식별자를 소유하지 않는다

### Status

Accepted

### Context

사용자 관점에서 중요한 것은 주문이 접수되었는지, 체결되었는지, 취소되었는지, 만료되었는지다.
어떤 브로커를 통해 처리되었는지는 사용자-facing 주문 상태 판단의 핵심 정보가 아니다.

또한 Broker Gateway가 외부 브로커 통신을 전담한다면, 브로커 코드와 브로커 주문 ID는 Gateway의 책임으로 두는 것이 경계가 더 명확하다.

### Decision

Order 도메인 모델은 `brokerCode`, `brokerOrderId`를 소유하지 않는다.

* Order Service는 `orderId` 기준으로 주문 상태를 변경한다.
* Broker Gateway는 브로커 binding, 브로커 주문 ID, 전문 송수신 이력을 소유한다.
* Broker Gateway는 canonical event에 `orderId`를 포함해 Order Service에 전달한다.

### Consequences

장점:

* Order Service가 브로커 세부 정보를 모르게 된다.
* 브로커 추가/교체 시 Order 도메인 모델이 흔들리지 않는다.
* 브로커 송수신 이력의 소유권이 Gateway로 명확해진다.

단점:

* 운영자가 주문 상태와 브로커 전문 이력을 한 번에 보려면 Order Service와 Gateway 데이터를 연결해야 한다.
* 운영 콘솔 단계에서 cross-service 조회 또는 별도 read model이 필요할 수 있다.

---

## DDR-008. Phase 1 브로커 전문은 `orderId`를 주문 참조값으로 포함한다

### Status

Accepted

### Context

브로커와 우리 시스템 사이의 주문 귀속을 위해 별도의 `brokerClientOrderId`를 둘 수도 있다.
하지만 Phase 1의 Broker Simulator는 우리가 제어하는 test double이며, 실제 외부 브로커 프로토콜을 완전히 재현하는 것이 목적이 아니다.

이 프로젝트의 Phase 1 핵심은 TCP framing, fixed-length 전문, 지연/중복/순서 역전/malformed, 주문 상태 수렴을 검증하는 것이다.

### Decision

Phase 1에서는 브로커 전문에 내부 `orderId`를 주문 참조값으로 포함한다.

* Gateway가 브로커로 주문/취소/상태조회 전문을 보낼 때 `orderId`를 포함한다.
* Broker Simulator는 후속 응답/이벤트에 `orderId`를 포함한다.
* Gateway는 이를 사용해 canonical broker event의 대상 주문을 식별한다.
* 별도의 `brokerClientOrderId`는 두지 않는다.
* `brokerOrderId`는 브로커가 부여한 외부 식별자로 Gateway가 관리한다.

### Consequences

장점:

* Phase 1에서 주문 귀속 로직이 단순해진다.
* 별도 client-side broker order reference를 만들 필요가 없다.
* Gateway가 canonical event에 `orderId`를 안정적으로 포함할 수 있다.

단점:

* 실제 외부 브로커 연계에서는 내부 `orderId`를 그대로 노출하지 않을 수 있다.
* 향후 실제 브로커 연계 또는 더 현실적인 멀티 브로커 시나리오에서는 별도 external order reference를 도입할 수 있다.

---

## DDR-009. 부분체결 후 취소는 체결분 확정 + 미체결 잔량 취소로 처리한다

### Status

Accepted

### Decision

부분체결 후 `CANCEL` instruction은 이미 체결된 수량을 취소하지 않는다.
미체결 잔량에 대해서만 취소를 요청한다.

예시:

```text id="b0lulh"
orderQty = 100
cumQty = 40
leavesQty = 60

CANCEL instruction 완료 이후:
status = CANCELED
cumQty = 40
leavesQty = 0
```

### Consequences

장점:

* 실제 주문 처리 의미와 맞다.
* `CANCELED` 상태에서도 `cumQty > 0`인 의미를 설명할 수 있다.

단점:

* `CANCELED`를 “전체 주문 취소”로 오해하지 않도록 문서화가 필요하다.

---

## DDR-010. 식별 불가능한 malformed 전문은 주문 상태를 직접 변경하지 않는다

### Status

Accepted

### Context

frame/header 오류로 `orderId`, `wireMessageId` 등을 읽을 수 없는 경우 특정 주문에 귀속할 수 없다.

### Decision

식별 불가능한 malformed 전문은 주문 상태를 직접 변경하지 않는다.

대신:

* protocol anomaly로 기록한다.
* malformed metric을 증가시킨다.
* 필요 시 connection close한다.
* 미확정 non-terminal 주문 탐지와 reconciliation으로 간접 복구한다.

### Consequences

장점:

* 잘못된 주문 상태 변경을 방지한다.
* 정보 부족 상황에서 위험한 추론을 하지 않는다.

단점:

* terminal event 유실을 즉시 알 수 없다.
* stale order detection과 EOD sweep이 필요하다.

---

## DDR-011. `NOT_FOUND` reconciliation 결과는 자동 종결하지 않는다

### Status

Accepted

### Context

브로커 상태조회 결과 `NOT_FOUND`는 여러 의미를 가질 수 있다.

* 실제로 주문이 도달하지 않음
* 조회 시점 문제
* 식별자 문제
* 브로커 상태 저장 지연

### Decision

Phase 1에서는 `NOT_FOUND`를 자동으로 `REJECTED`나 `EXPIRED`로 종결하지 않는다.

기본 처리:

* `Order.status = UNKNOWN` 유지
* `reconciliationStatus = FAILED`
* 실패 사유 기록
* 운영 추적 대상으로 남김

### Consequences

장점:

* 잘못된 자동 종결을 피한다.
* 운영자가 확인할 수 있는 안전한 상태로 남긴다.

단점:

* 일부 주문이 자동 종결되지 않고 남을 수 있다.
* 운영 처리 또는 후속 정책이 필요하다.

---

# 8.7 확정 사항 요약

| 항목                 | 결정                                                                                                                  |
| ------------------ | ------------------------------------------------------------------------------------------------------------------- |
| 서비스 분리             | Order Service / Broker Gateway Service / Recovery Service                                                           |
| 주문 상태 소유권          | Order Service 단독 소유                                                                                                 |
| instruction 상태 소유권 | Order Service가 `OrderInstruction` 상태를 소유                                                                            |
| 메시징 방식             | Kafka                                                                                                               |
| 발행 신뢰성             | Transactional Outbox                                                                                                |
| 소비 중복 방지           | processed message + broker event semantic dedup                                                                     |
| 외부 브로커 통신          | Broker Gateway에 격리                                                                                                  |
| 브로커 식별자 소유권        | Broker Gateway가 `brokerCode`, `brokerOrderId`, broker binding을 소유                                                   |
| 브로커 전문 주문 참조       | Phase 1에서는 `orderId`를 전문에 포함                                                                                        |
| 복구 방식              | `UNKNOWN` + Recovery Service reconciliation                                                                         |
| Broker Simulator   | Netty 기반 TCP fixed-length message server                                                                            |
| 실시간 알림             | Order Service의 SSE                                                                                                  |
| 주문 상태 모델           | `PENDING_ACK`, `LIVE`, `PARTIALLY_FILLED`, `PENDING_CANCEL`, `UNKNOWN`, `FILLED`, `CANCELED`, `REJECTED`, `EXPIRED` |
| instruction 모델     | `OrderInstruction`으로 일반화. Phase 1에서는 `PLACE`, `CANCEL` 지원                                                           |
| 체결 모델              | 가격 제외, 수량 중심                                                                                                        |
| 중복 이벤트 기준          | Gateway가 부여한 opaque `brokerEventDedupKey`                                                                           |
| 부분체결 후 취소          | 체결분 확정 + 미체결 잔량 취소                                                                                                  |
| malformed 식별 불가 전문 | 주문 상태 직접 변경 금지                                                                                                      |
| `NOT_FOUND`        | 자동 종결 금지                                                                                                            |
