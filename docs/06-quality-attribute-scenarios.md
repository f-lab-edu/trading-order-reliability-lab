# 6. 품질 속성 시나리오

## 6.1 목적

품질 속성 시나리오는 시스템이 특정 상황에서 어떤 품질 기준을 만족해야 하는지 검증 가능하게 정의하는 문서다.

이 문서는 구체 구현 기술을 결정하지 않는다.
대신 다음 질문에 답한다.

1. 정상 접수된 주문/취소 요청이 중간 장애로 사라지지 않는가?
2. 외부 브로커 이벤트가 중복되거나 순서가 꼬여도 주문 상태와 수량이 오염되지 않는가?
3. 외부 응답이 불확실할 때 시스템은 이를 어떻게 격리하고 복구 대상으로 식별하는가?
4. 식별 불가능한 전문 오류나 이벤트 유실로 인해 주문이 non-terminal 상태에 방치되지 않는가?
5. 외부 브로커 장애가 사용자-facing 조회 기능 전체로 전파되지 않는가?
6. 특정 주문이 왜 현재 상태가 되었는지 추적할 수 있는가?
7. 향후 브로커 추가나 통신 방식 변경에도 주문 도메인 모델이 유지되는가?
8. 장애, 중복, 순서 역전, malformed, 이벤트 유실 시나리오를 재현 가능하게 검증할 수 있는가?

---

## 6.2 품질 속성 정의

| 품질 속성                            | 의미                                                   |
| -------------------------------- | ---------------------------------------------------- |
| `Reliability`                    | 정상 접수된 주문/취소 요청이 중간 장애로 사라지지 않는 것                    |
| `Consistency`                    | 중복/순서 역전 이벤트에도 주문 상태와 수량이 잘못 변경되지 않는 것               |
| `Recoverability`                 | 불확실하거나 미확정된 주문 상태를 복구 대상으로 식별하고 최종 상태로 수렴시키는 것       |
| `Availability / Fault Isolation` | 외부 브로커 장애가 사용자 조회 기능 전체로 전파되지 않는 것                   |
| `Observability / Operability`    | 주문 상태 변화의 원인과 외부 상호작용을 추적할 수 있는 것                    |
| `Timeliness`                     | 주문 상태 변화가 사용자가 의미 있게 인지할 수 있는 시간 안에 반영되는 것           |
| `Modifiability`                  | 브로커 추가/교체나 통신 방식 변경에도 주문 도메인 모델이 안정적인 것              |
| `Testability`                    | 장애, 중복, 순서 역전, malformed, 이벤트 유실을 재현 가능하게 검증할 수 있는 것 |

---

## 6.3 품질 속성 시나리오

## QA-001. 정상 접수된 주문 요청은 중간 장애로 사라지면 안 된다

| 항목                | 내용                                                                                          |
| ----------------- | ------------------------------------------------------------------------------------------- |
| Quality Attribute | Reliability                                                                                 |
| Source            | Retail Investor                                                                             |
| Stimulus          | 사용자가 주문 생성 요청을 보내고 시스템이 정상 접수 응답을 반환한다                                                      |
| Environment       | 주문 접수 직후 내부 처리 과정에서 일부 처리 실패 또는 일시적 장애가 발생한다                                                |
| Artifact          | Order Intake, Order Processing Flow, External Broker Submission Flow                        |
| Response          | 시스템은 사용자의 주문 요청을 보존해야 한다. 외부 브로커로 주문을 전달하거나, 전달 여부가 불확실하면 사용자가 인지 가능한 복구 대상 상태로 전환해야 한다     |
| Response Measure  | 정상 접수된 주문이 추적 불가능하게 사라지지 않는다. 주문은 `PENDING_ACK`, `UNKNOWN`, 또는 terminal 상태 중 하나로 조회 가능해야 한다 |

### 관련 유스케이스

* `UC-001` 신규 주문 생성
* `UC-011` 응답 timeout 후 UNKNOWN 처리
* `UC-012` Reconciliation으로 상태 수렴

---

## QA-002. 정상 접수된 취소 요청은 장애 이후에도 최종 결과로 수렴해야 한다

| 항목 | 내용 |
|---|---|
| Quality Attribute | Reliability, Recoverability |
| Source | Retail Investor |
| Stimulus | 사용자가 미체결 잔량 취소를 요청했고, 시스템은 이를 `CANCEL` instruction으로 정상 접수했다 |
| Environment | 취소 instruction 접수 이후 외부 브로커 응답이 timeout되거나 유실된다 |
| Artifact | Cancel Instruction Handling, Order State Machine, Recovery Flow |
| Response | 시스템은 사용자의 취소 의도를 잃지 않아야 한다. 상태 확인 결과 주문이 여전히 활성 상태라면 사용자의 재입력 없이 취소를 다시 수행해야 한다 |
| Response Measure | 사용자가 취소 버튼을 다시 누르지 않아도 취소 처리가 재시도된다. 최종적으로 `CANCELED`, `FILLED`, `EXPIRED`, 또는 `UNKNOWN + FAILED` 중 하나로 수렴한다 |

### 관련 유스케이스

* `UC-007` 주문 취소 요청
* `UC-013` 취소 의도 유지 후 자동 재시도

---

## QA-003. 동일 외부 이벤트는 주문 상태에 한 번만 반영되어야 한다

| 항목                | 내용                                                                             |
| ----------------- | ------------------------------------------------------------------------------ |
| Quality Attribute | Consistency                                                                    |
| Source            | External Broker Simulator                                                      |
| Stimulus          | 동일한 주문 접수, 체결, 취소, 만료 이벤트가 여러 번 전달된다                                           |
| Environment       | 외부 브로커 재전송 또는 내부 재처리 상황                                                        |
| Artifact          | Broker Event Handling, Order State Machine                                     |
| Response          | 시스템은 같은 논리 이벤트를 중복으로 판단하고 주문 상태와 수량을 한 번만 변경해야 한다                              |
| Response Measure  | 동일 체결 이벤트가 여러 번 도착해도 `cumQty`, `leavesQty`는 한 번만 변경된다. 중복 이벤트는 추적 가능한 이력으로 남는다 |

### 관련 유스케이스

* `UC-006` 부분체결/완전체결 반영
* `UC-014` 중복 브로커 이벤트 처리

---

## QA-004. 외부 이벤트가 순서대로 오지 않아도 주문 상태가 수렴해야 한다

| 항목                | 내용                                                                                                   |
| ----------------- | ---------------------------------------------------------------------------------------------------- |
| Quality Attribute | Consistency                                                                                          |
| Source            | External Broker Simulator                                                                            |
| Stimulus          | 주문 접수보다 체결 이벤트가 먼저 오거나, 취소 요청 중 체결 이벤트가 먼저 도착한다                                                      |
| Environment       | 외부 브로커 이벤트 순서 역전 상황                                                                                  |
| Artifact          | Order State Machine                                                                                  |
| Response          | 시스템은 이벤트가 도착한 순서가 아니라 주문 수량과 상태 전이 규칙을 기준으로 유효한 상태로 수렴해야 한다                                          |
| Response Measure  | ACK보다 먼저 체결 이벤트가 와도 주문은 `PARTIALLY_FILLED` 또는 `FILLED`로 수렴한다. 늦은 ACK나 늦은 CancelAck는 종결 상태를 오염시키지 않는다 |

### 관련 유스케이스

* `UC-015` 순서 역전 브로커 이벤트 처리

---

## QA-005. 외부 응답이 불확실하면 실패로 단정하지 않고 복구 가능한 상태로 격리해야 한다

| 항목                | 내용                                                                                |
| ----------------- | --------------------------------------------------------------------------------- |
| Quality Attribute | Recoverability                                                                    |
| Source            | External Broker Simulator                                                         |
| Stimulus          | 주문 또는 취소 요청 이후 외부 브로커 응답이 정해진 시간 안에 도착하지 않는다                                      |
| Environment       | 외부 브로커 응답 지연, 응답 유실, 연결 불안정 상황                                                    |
| Artifact          | Order State Machine, Recovery Flow                                                |
| Response          | 시스템은 주문을 실패로 단정하지 않고 `UNKNOWN`으로 격리한 뒤 상태조회 기반 복구 대상으로 등록해야 한다                    |
| Response Measure  | 주문은 `REJECTED`, `CANCELED` 등으로 임의 종결되지 않는다. 사용자는 주문이 확인 중임을 조회할 수 있다. 복구 절차가 시작된다 |

### 관련 유스케이스

* `UC-011` 응답 timeout 후 UNKNOWN 처리
* `UC-012` Reconciliation으로 상태 수렴

---

## QA-006. 미확정 non-terminal 주문은 방치되지 않고 복구 대상으로 식별되어야 한다

| 항목                | 내용                                                                                       |
| ----------------- | ---------------------------------------------------------------------------------------- |
| Quality Attribute | Recoverability, Operability                                                              |
| Source            | External Broker Simulator, Recovery Flow                                                 |
| Stimulus          | terminal event가 유실되거나 식별 불가능한 malformed 전문으로 인해 특정 주문이 non-terminal 상태에 오래 머문다           |
| Environment       | 주문이 `LIVE`, `PARTIALLY_FILLED`, `PENDING_CANCEL`, `UNKNOWN` 등으로 남아 있는 상황                 |
| Artifact          | Order Lifecycle Management, Recovery Flow                                                |
| Response          | 시스템은 미확정 non-terminal 주문을 복구 대상으로 식별하고 상태조회 기반 수렴 절차를 수행해야 한다                            |
| Response Measure  | 주문이 영구적으로 non-terminal 상태에 방치되지 않는다. 상태조회 결과에 따라 terminal 상태 또는 `UNKNOWN + FAILED`로 정리된다 |

### 관련 유스케이스

* `UC-016` Malformed 전문 처리
* `UC-017` Stale Order Detection and Reconciliation

### 비고

식별 불가능한 malformed 전문은 특정 주문에 직접 연결할 수 없으므로 주문 상태를 직접 변경하지 않는다.
대신 그로 인해 발생할 수 있는 미확정 non-terminal 주문을 별도 복구 대상으로 식별한다.

---

## QA-007. 외부 브로커 장애는 사용자 조회 기능을 마비시키면 안 된다

| 항목                | 내용                                                                     |
| ----------------- | ---------------------------------------------------------------------- |
| Quality Attribute | Availability / Fault Isolation                                         |
| Source            | External Broker Simulator                                              |
| Stimulus          | 외부 브로커가 응답하지 않거나 연결 장애가 발생한다                                           |
| Environment       | 사용자가 주문 상세 또는 주문 목록을 조회한다                                              |
| Artifact          | User-facing Order Query                                                |
| Response          | 시스템은 외부 브로커에 동기 의존하지 않고 사용자가 마지막으로 확정된 주문 상태를 조회할 수 있게 해야 한다           |
| Response Measure  | 브로커 장애 중에도 주문 조회는 실패하지 않는다. 단, 상태가 불확실한 주문은 `UNKNOWN` 또는 확인 중 상태로 표현된다 |

### 관련 유스케이스

* `UC-003` 주문 상태 조회
* `UC-011` 응답 timeout 후 UNKNOWN 처리

---

## QA-008. 주문 상태 변화의 원인은 추적 가능해야 한다

| 항목                | 내용                                                                |
| ----------------- | ----------------------------------------------------------------- |
| Quality Attribute | Observability / Operability                                       |
| Source            | Operations Engineer                                               |
| Stimulus          | 특정 주문이 예상과 다른 상태가 되어 원인 분석이 필요하다                                  |
| Environment       | 운영 또는 테스트 환경                                                      |
| Artifact          | Order History, Broker Interaction History, Recovery History       |
| Response          | 운영자는 주문 생성부터 외부 브로커 상호작용, 상태 전이, 복구 시도까지의 주요 사건을 연결해 확인할 수 있어야 한다 |
| Response Measure  | 특정 주문 또는 추적 식별자를 기준으로 상태 변경 원인, 외부 상호작용, 복구 결과를 확인할 수 있다          |

### 관련 유스케이스

* `UC-018` 주문 장애 추적

---

## QA-009. 주문 상태 변화는 사용자가 인지 가능한 시간 안에 반영되어야 한다

| 항목                | 내용                                                                |
| ----------------- | ----------------------------------------------------------------- |
| Quality Attribute | Timeliness                                                        |
| Source            | External Broker Simulator                                         |
| Stimulus          | 주문 접수, 부분체결, 완전체결, 취소, 만료 이벤트가 발생한다                               |
| Environment       | 정상 처리 흐름                                                          |
| Artifact          | User-facing Order Status View                                     |
| Response          | 시스템은 상태 변화를 사용자가 인지 가능한 시간 안에 조회 결과와 실시간 알림에 반영해야 한다              |
| Response Measure  | 상태 변화가 과도하게 지연되지 않아야 한다. 구체적인 지연 기준과 측정 방식은 테스트/모니터링 계획 단계에서 정의한다 |

### 관련 유스케이스

* `UC-004` 주문 상태 실시간 구독
* `UC-005` 브로커 주문 접수/거절 반영
* `UC-006` 부분체결/완전체결 반영

---

## QA-010. 브로커 추가/교체 시 주문 도메인 모델은 안정적이어야 한다

| 항목                | 내용                                                                |
| ----------------- | ----------------------------------------------------------------- |
| Quality Attribute | Modifiability                                                     |
| Source            | Developer                                                         |
| Stimulus          | 새로운 외부 브로커 또는 새로운 통신 방식이 추가된다                                     |
| Environment       | Phase 2 확장 또는 후속 기능 개발                                            |
| Artifact          | Order Domain Model, External Broker Integration Boundary          |
| Response          | 주문 상태 모델과 핵심 상태 전이 규칙은 유지하고, 외부 브로커 통신 방식과 라우팅 정책만 확장 가능해야 한다     |
| Response Measure  | 신규 브로커 추가 시 `OrderStatus`, 주요 상태 전이, 사용자-facing 주문 API가 재설계되지 않는다 |

### 관련 확장 범위

* 멀티 브로커 라우팅
* 브로커 fallback
* 향후 다른 브로커 통신 방식 추가

---

## QA-011. 장애 시나리오는 재현 가능해야 한다

| 항목                | 내용                                                                 |
| ----------------- | ------------------------------------------------------------------ |
| Quality Attribute | Testability                                                        |
| Source            | Test Scenario Controller                                           |
| Stimulus          | 테스트 주체가 지연, 유실, 중복, 순서 역전, malformed, terminal event 유실 시나리오를 설정한다 |
| Environment       | 로컬 또는 통합 테스트 환경                                                    |
| Artifact          | External Broker Simulator, Test Harness                            |
| Response          | 시스템은 동일한 장애 시나리오를 반복 가능하게 재현하고, 상태 수렴 결과를 검증할 수 있어야 한다             |
| Response Measure  | 주요 장애 시나리오가 자동화 테스트로 재현 가능하다                                       |

### 관련 유스케이스

* `UC-014` 중복 브로커 이벤트 처리
* `UC-015` 순서 역전 브로커 이벤트 처리
* `UC-016` Malformed 전문 처리
* `UC-017` Stale Order Detection and Reconciliation

---

## 6.4 우선순위

## Must

| ID       | 품질 속성 시나리오                                    |
| -------- | --------------------------------------------- |
| `QA-001` | 정상 접수된 주문 요청은 중간 장애로 사라지면 안 된다                |
| `QA-002` | 정상 접수된 취소 요청은 장애 이후에도 최종 결과로 수렴해야 한다          |
| `QA-003` | 동일 외부 이벤트는 주문 상태에 한 번만 반영되어야 한다               |
| `QA-004` | 외부 이벤트가 순서대로 오지 않아도 주문 상태가 수렴해야 한다            |
| `QA-005` | 외부 응답이 불확실하면 실패로 단정하지 않고 복구 가능한 상태로 격리해야 한다   |
| `QA-006` | 미확정 non-terminal 주문은 방치되지 않고 복구 대상으로 식별되어야 한다 |
| `QA-007` | 외부 브로커 장애는 사용자 조회 기능을 마비시키면 안 된다              |
| `QA-008` | 주문 상태 변화의 원인은 추적 가능해야 한다                      |
| `QA-011` | 장애 시나리오는 재현 가능해야 한다                           |

---

## Should

| ID       | 품질 속성 시나리오                           |
| -------- | ------------------------------------ |
| `QA-009` | 주문 상태 변화는 사용자가 인지 가능한 시간 안에 반영되어야 한다 |
| `QA-010` | 브로커 추가/교체 시 주문 도메인 모델은 안정적이어야 한다     |

---

## 6.5 이후 설계 단계로 연결되는 질문

이 품질 속성 시나리오는 이후 아키텍처와 ADR에서 다음 질문으로 이어진다.

| 품질 속성                          | 이후 설계 질문                                                    |
| ------------------------------ | ----------------------------------------------------------- |
| Reliability                    | 정상 접수된 주문 생성/취소 instruction을 어떤 구조로 보존하고 재시도 가능하게 만들 것인가? |
| Consistency                    | 동일 외부 이벤트의 논리적 동일성을 어떻게 식별하고, 중복 반영을 어떻게 막을 것인가?            |
| Consistency                    | 어떤 순서 역전 이벤트를 허용하고, 어떤 이벤트는 거부하거나 복구 대상으로 볼 것인가?            |
| Recoverability                 | 어떤 조건에서 주문을 `UNKNOWN`으로 격리할 것인가?                            |
| Recoverability                 | `UNKNOWN` 또는 미확정 non-terminal 주문을 누가, 언제, 어떤 방식으로 수렴시킬 것인가? |
| Availability / Fault Isolation | 외부 브로커 장애를 사용자 조회 경로와 어떻게 분리할 것인가?                          |
| Observability / Operability    | 주문 상태 변화, 외부 상호작용, 복구 시도를 어떤 식별자와 이력으로 연결할 것인가?             |
| Timeliness                     | 어느 구간의 지연을 측정하고 어떤 기준으로 회귀를 판단할 것인가?                        |
| Modifiability                  | 브로커 통신 방식과 주문 도메인 모델을 어떻게 분리할 것인가?                          |
| Testability                    | 장애 시나리오를 어떻게 결정적으로 재현할 것인가?                                 |

---

## 6.6 확정 사항 요약

| 항목                         | 결정                                                                                                               |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 구현 기술 직접 언급                | 6단계에서는 사용하지 않음                                                                                                   |
| 성능 수치                      | 12단계 테스트/모니터링 계획으로 이동                                                                                            |
| 보안 검증                      | 3단계 최소 보안 NFR 및 12단계 테스트 계획에서 다룸                                                                                 |
| 사용자 의도 보존                  | 별도 품질 속성명이 아니라 Reliability / Recoverability로 흡수                                                                  |
| stale detector / EOD sweep | 구체 정책명 대신 “미확정 non-terminal 주문 복구”로 추상화                                                                          |
| 핵심 품질 속성                   | Reliability, Consistency, Recoverability, Fault Isolation, Observability, Timeliness, Modifiability, Testability |
