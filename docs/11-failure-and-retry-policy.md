# 11. 장애 처리 / 재처리 정책

## 11.1 목적

이 문서는 주문 처리 과정에서 발생할 수 있는 장애와 재처리 정책을 정의한다.

이 문서는 다음 질문에 답한다.

1. 어떤 상황을 장애 또는 미확정 상태로 볼 것인가?
2. 어떤 장애는 즉시 실패 처리하고, 어떤 장애는 `UNKNOWN` 또는 reconciliation 대상으로 격리할 것인가?
3. 어떤 작업은 자동 재시도하고, 어떤 작업은 재시도하지 않을 것인가?
4. 중복 메시지, 중복 브로커 이벤트, 순서 역전 이벤트를 어떻게 처리할 것인가?
5. malformed 전문이 주문 상태를 오염시키지 않도록 어떻게 격리할 것인가?
6. reconciliation job과 attempt는 언제 재시도하고, 언제 실패로 종결할 것인가?
7. Recovery workflow failure와 Order domain resolution failure를 어떻게 구분할 것인가?
8. 운영자가 추적할 수 있도록 어떤 이력을 남겨야 하는가?

---

## 11.2 기본 원칙

### 11.2.1 불확실한 외부 command 결과는 실패로 단정하지 않는다

외부 브로커로 command를 보냈지만 결과를 확정할 수 없는 경우, 시스템은 주문을 임의로 성공 또는 실패 처리하지 않는다.

예:

* 주문 요청 후 ACK/Reject 미수신
* 취소 요청 후 CancelAck/CancelReject 미수신
* pending command 응답으로 보이나 body 파싱 실패
* 브로커 연결 종료로 결과 확인 불가

이 경우 Order Service는 주문을 `UNKNOWN`으로 격리하고 reconciliation 대상으로 전환한다.

---

### 11.2.2 stale 상태와 `UNKNOWN`은 다르다

`UNKNOWN`은 외부 command의 업무 결과를 확정할 수 없다는 의미다.

반면 `LIVE`, `PARTIALLY_FILLED` 상태에서 오래 변화가 없는 경우는 마지막으로 확정된 주문 상태가 존재한다. 이런 경우에는 주문 상태를 즉시 `UNKNOWN`으로 바꾸지 않고, 기존 상태를 유지한 채 `reconciliationStatus = PENDING`으로 전환하여 상태 검증을 수행한다.

| 상황                               | 기본 처리                        |
| -------------------------------- | ---------------------------- |
| `PENDING_ACK`에서 submit 결과 불확실    | `UNKNOWN + PENDING`          |
| `PENDING_CANCEL`에서 cancel 결과 불확실 | `UNKNOWN + PENDING`          |
| `LIVE` stale                     | `LIVE + PENDING`             |
| `PARTIALLY_FILLED` stale         | `PARTIALLY_FILLED + PENDING` |
| EOD 이후 non-terminal DAY 주문       | 기존 상태 유지 + `PENDING` 우선      |

---

### 11.2.3 주문 상태와 instruction 상태는 Order Service만 변경한다

Broker Gateway와 Recovery Service는 주문 상태를 직접 변경하지 않는다.

* Broker Gateway는 브로커 전문을 canonical broker event로 변환한다.
* Recovery Service는 reconciliation workflow를 수행한다.
* Order Service는 broker event, lifecycle event, recovery event를 상태머신에 적용한다.

---

### 11.2.4 Recovery Service는 snapshot을 해석하지 않는다

Recovery Service는 브로커 상태조회 workflow를 관리하지만, 브로커 snapshot을 직접 해석하지 않는다.

상태조회 흐름은 다음과 같다.

```text
Recovery Service
  -> QueryOrderStatusCommand 발행

Broker Gateway
  -> 브로커 상태조회 전문 송신
  -> BrokerOrderStatusSnapshot 발행

Order Service
  -> snapshot을 주문 상태머신에 적용
  -> OrderReconciliationResolved 또는 OrderReconciliationFailed 발행

Recovery Service
  -> 결과 이벤트를 보고 job / attempt 종료
```

즉, Recovery Service의 `reconciliation_attempt`는 snapshot 저장소가 아니라 **상태조회 workflow 시도 이력**이다.

---

### 11.2.5 자동 재시도는 안전한 작업에만 수행한다

무조건 재전송하면 안 된다.

특히 브로커 command는 이미 외부 브로커에 도달했을 수 있으므로, timeout만 보고 같은 command를 즉시 재전송하면 중복 주문이나 잘못된 취소가 발생할 수 있다.

| 대상                  |     자동 재시도 여부 | 기준                                                     |
| ------------------- | ------------: | ------------------------------------------------------ |
| 메시지 발행              |             Y | Outbox 기반 재시도                                          |
| 메시지 소비              |             Y | processed message로 멱등 처리                               |
| 사용자 주문 생성 요청        | Client 재시도 허용 | `clientOrderId` 멱등성으로 방어                               |
| 사용자 취소 요청           | Client 재시도 허용 | `clientCancelRequestId` 멱등성으로 방어                       |
| 주문 submit 결과 불확실    |     직접 재전송 금지 | `UNKNOWN` 후 상태조회                                       |
| 취소 결과 불확실           |     직접 재전송 금지 | `UNKNOWN` 후 상태조회, 활성 주문 확인 시 Order Service가 cancel 재발행 |
| reconciliation 상태조회 |             Y | job / attempt 기준 재시도                                   |
| SSE 전송              |           제한적 | 상태 저장소가 source of truth이므로 best-effort                 |

---

### 11.2.6 모든 재처리는 멱등해야 한다

시스템은 at-least-once 메시징을 전제로 한다.

따라서 다음을 보장해야 한다.

* 같은 message envelope는 한 번만 비즈니스 처리한다.
* 같은 외부 브로커 사건은 주문 상태에 한 번만 반영한다.
* 같은 사용자 instruction은 한 번만 생성한다.
* 같은 주문에 동시에 여러 active `CANCEL` instruction을 허용하지 않는다.

---

## 11.3 장애 유형 분류

| 분류                              | 예시                                                | 기본 처리                                  |
| ------------------------------- | ------------------------------------------------- | -------------------------------------- |
| 사용자 요청 오류                       | 잘못된 수량, 잘못된 가격, 미지원 주문 타입                         | 즉시 거절                                  |
| 사용자 요청 중복                       | 동일 `clientOrderId`, 동일 `clientCancelRequestId`    | 멱등 처리                                  |
| 사용자 요청 충돌                       | 같은 멱등성 키 + 다른 payload                             | `409 Conflict`                         |
| 메시지 발행 실패                       | Kafka publish 실패                                  | Outbox 재시도                             |
| 메시지 중복 소비                       | 같은 `messageId` 재전달                                | processed message로 무시                  |
| 외부 사건 중복                        | 같은 broker event가 다른 message로 재전달                  | `brokerEventDedupKey`로 무시              |
| 브로커 명시적 거절                      | 주문 거절, 취소 거절                                      | 도메인 상태로 반영                             |
| 브로커 command 결과 불확실              | ACK/CancelAck timeout, pending response malformed | `UNKNOWN + reconciliation`             |
| 순서 역전 이벤트                       | ACK보다 Fill 먼저 도착                                  | 상태머신 규칙에 따라 수렴                         |
| malformed 전문                    | frame/header/body 오류                              | Gateway에서 격리, 상태 직접 변경 금지              |
| stale non-terminal              | 오래 머문 활성 주문                                       | Order Service가 reconciliation 요청       |
| EOD non-terminal                | 장 마감 이후 DAY 주문이 terminal 아님                       | Order Service가 reconciliation 요청       |
| reconciliation workflow failure | 상태조회 재시도 한도 초과                                    | Recovery가 `ReconciliationJobFailed` 발행 |
| domain resolution failure       | snapshot을 주문 상태로 수렴 불가                            | Order가 `OrderReconciliationFailed` 발행  |

---

## 11.4 사용자 요청 처리 정책

## 11.4.1 주문 생성 요청

### 정상 처리

사용자가 주문 생성 요청을 보내면 Order Service는 `PLACE` instruction을 생성하고 주문을 `PENDING_ACK` 상태로 생성한다.

### 중복 처리

| 조건                                          | 처리                |
| ------------------------------------------- | ----------------- |
| 동일 `accountId + clientOrderId` + 동일 payload | 기존 주문 생성 결과 반환    |
| 동일 `accountId + clientOrderId` + 다른 payload | `409 Conflict`    |
| 동일 요청 재시도 중 기존 주문이 `PENDING_ACK`            | 기존 상태 반환          |
| 동일 요청 재시도 중 기존 주문이 terminal                 | 기존 terminal 상태 반환 |

### 실패 처리

| 조건             | 처리                                      |
| -------------- | --------------------------------------- |
| 수량이 0 이하       | `400 Bad Request`                       |
| 지정가가 0 이하      | `400 Bad Request`                       |
| 미지원 주문 타입      | `400 Bad Request`                       |
| 시장 상태 `CLOSED` | Phase 1에서는 요청 거절                        |
| 내부 저장 실패       | 요청 실패. 단, commit 전이면 주문은 생성되지 않은 것으로 본다 |

---

## 11.4.2 취소 요청

### 정상 처리

사용자가 취소 요청을 보내면 Order Service는 대상 주문을 잠그고 active `CANCEL` instruction 존재 여부를 확인한다.

### 처리 정책

| 조건                                              | 처리                                           |
| ----------------------------------------------- | -------------------------------------------- |
| 주문 상태가 `LIVE`                                   | `CANCEL` instruction 생성, `PENDING_CANCEL` 전환 |
| 주문 상태가 `PARTIALLY_FILLED`                       | `CANCEL` instruction 생성, `PENDING_CANCEL` 전환 |
| 주문 상태가 `PENDING_ACK`                            | 취소 허용. `PENDING_CANCEL` 전환                   |
| 주문 상태가 `UNKNOWN`                                | 취소 요청 거절                                     |
| 주문이 terminal 상태                                 | 취소 요청 거절                                     |
| 동일 `clientCancelRequestId` 재요청                  | 기존 취소 instruction 결과 반환                      |
| 다른 `clientCancelRequestId`이나 active `CANCEL` 존재 | `409 Conflict`                               |

---

## 11.5 메시지 발행 실패 정책

## 11.5.1 Outbox 기반 발행

메시지를 발행해야 하는 서비스는 업무 상태 변경과 발행 대상 메시지 저장을 같은 로컬 트랜잭션에서 처리한다.

적용 서비스:

* Order Service
* Broker Gateway Service
* Recovery Service

### Outbox 상태

| 상태           | 의미                      |
| ------------ | ----------------------- |
| `READY`      | 발행 대기                   |
| `PUBLISHING` | publisher가 claim하여 발행 중 |
| `SENT`       | 발행 완료                   |
| `FAILED`     | 발행 실패 후 재시도 대기          |

---

## 11.5.2 발행 재시도

발행 실패 시 다음을 수행한다.

1. `retry_count` 증가
2. `next_retry_at` 설정
3. `last_error` 기록
4. 상태를 `FAILED`로 변경
5. 이후 publisher가 재시도

### 재시도 정책

| 항목        | 정책                              |
| --------- | ------------------------------- |
| 재시도 방식    | exponential backoff + jitter 권장 |
| 최대 재시도 횟수 | 12단계 테스트/운영 계획에서 수치 확정          |
| 영구 실패 처리  | 운영 추적 대상으로 남김                   |
| 중복 발행 가능성 | 허용. consumer idempotency로 방어    |

---

## 11.5.3 publisher 장애

publisher가 메시지를 claim한 뒤 장애로 종료될 수 있다.

이 경우 `locked_until`이 만료되면 다른 publisher가 메시지를 다시 claim할 수 있다.

중요한 한계:

> Kafka publish는 성공했지만 `SENT` 갱신 전에 장애가 발생하면 같은 메시지가 중복 발행될 수 있다.

따라서 consumer는 반드시 멱등하게 동작해야 한다.

---

## 11.6 메시지 소비 실패 정책

## 11.6.1 processed message 기록

각 consumer는 메시지를 처리할 때 `consumerName + messageId` 기준으로 이미 처리한 메시지인지 확인한다.

| 상황       | 처리                      |
| -------- | ----------------------- |
| 처리 이력 없음 | 비즈니스 처리 수행              |
| 처리 이력 있음 | 중복 메시지로 보고 비즈니스 처리 생략   |
| 처리 중 장애  | DB commit 여부에 따라 재처리 가능 |

---

## 11.6.2 DB commit과 offset commit 순서

원칙:

> 비즈니스 DB transaction commit 이후 message acknowledgement 또는 offset commit을 수행한다.

이유:

* offset을 먼저 commit하면 비즈니스 처리 실패 시 메시지를 잃는다.
* DB commit 후 offset commit 실패는 메시지 재소비로 이어질 수 있지만 processed message로 방어 가능하다.

---

## 11.6.3 poison message

일시적 장애가 아니라 payload 구조 오류나 처리 불가능한 메시지가 반복될 수 있다.

Phase 1에서는 다음처럼 처리한다.

| 상황               | 처리                                |
| ---------------- | --------------------------------- |
| schema 자체가 파싱 불가 | consumer error log, metric 기록     |
| 도메인 규칙상 처리 불가    | 상태 반영 금지, order event 또는 운영 이력 기록 |
| 반복 실패            | 12단계에서 DLQ 또는 parking topic 검토    |

DLQ 구체 정책은 11장에서 완전히 확정하지 않고, 12장 운영/모니터링 계획에서 수치와 함께 정한다.

---

## 11.7 브로커 command 실패 정책

## 11.7.1 command 종류

| command        | 설명      |
| -------------- | ------- |
| `SUBMIT`       | 주문 요청   |
| `CANCEL`       | 취소 요청   |
| `QUERY_STATUS` | 상태조회 요청 |

---

## 11.7.2 command lifecycle

| 상태          | 의미                  |
| ----------- | ------------------- |
| `CREATED`   | command attempt 생성  |
| `SENT`      | TCP write 수행        |
| `ACKED`     | 기대한 업무 응답 수신        |
| `TIMED_OUT` | deadline 내 응답 없음    |
| `FAILED`    | 전송 전 또는 처리 중 명시적 실패 |
| `UNKNOWN`   | 결과 확정 불가            |

---

## 11.7.3 submit 결과 불확실

상황:

* `ORDR` 전송 후 `ACKN` 또는 `RJCT` 미수신
* 연결 종료로 결과 확인 불가
* pending submit 응답이 malformed라 업무 결과 해석 불가

Gateway 처리:

1. command attempt를 `TIMED_OUT` 또는 `UNKNOWN`으로 기록
2. `BrokerCommandOutcomeUnknown(commandType=SUBMIT)` 발행

Order Service 처리:

1. 주문을 `UNKNOWN`으로 전환
2. `reconciliationStatus = PENDING`
3. `OrderReconciliationRequested(triggerType=SUBMIT_OUTCOME_UNKNOWN)` 발행

---

## 11.7.4 cancel 결과 불확실

상황:

* `CXLQ` 전송 후 `CXLA` 또는 `CXLR` 미수신
* 연결 종료로 결과 확인 불가
* pending cancel 응답이 malformed라 업무 결과 해석 불가

Gateway 처리:

1. command attempt를 `TIMED_OUT` 또는 `UNKNOWN`으로 기록
2. `BrokerCommandOutcomeUnknown(commandType=CANCEL)` 발행

Order Service 처리:

1. 주문을 `UNKNOWN`으로 전환
2. active `CANCEL` instruction은 `REQUESTED` 유지
3. `reconciliationStatus = PENDING`
4. `OrderReconciliationRequested(triggerType=CANCEL_OUTCOME_UNKNOWN)` 발행

중요:

> cancel 결과가 불확실하다고 즉시 같은 cancel command를 재전송하지 않는다.
> 상태조회 결과 주문이 여전히 활성 상태임을 확인한 뒤 Order Service가 cancel command 재발행 여부를 결정한다.

---

## 11.7.5 query status 실패

상태조회 command는 Recovery Service의 reconciliation workflow 일부다.

`QUERY_STATUS` 실패는 주문 상태를 직접 바꾸지 않고 Recovery attempt 실패로 기록한다.

Recovery Service 처리:

1. attempt deadline 안에 `OrderReconciliationResolved` 또는 `OrderReconciliationFailed`를 받지 못하면 attempt를 `TIMED_OUT`으로 처리한다.
2. retry 가능하면 새 attempt를 생성한다.
3. retry 한도 초과 시 job을 `FAILED`로 변경한다.
4. `ReconciliationJobFailed` 이벤트를 Order Service에 발행한다.

Order Service 처리:

1. `ReconciliationJobFailed` 수신
2. 대상 주문의 `reconciliationStatus = FAILED`로 변경
3. 주문 상태 자체는 임의로 변경하지 않는다.
4. `OrderReconciliationWorkflowFailed` 이벤트 이력을 남긴다.

---

# 11.8 브로커 이벤트 처리 정책

## 11.8.1 중복 이벤트

같은 외부 브로커 사건은 주문 상태에 한 번만 반영한다.

기준:

```text
brokerEventDedupKey
```

| 상황                          | 처리                                      |
| --------------------------- | --------------------------------------- |
| 처음 보는 `brokerEventDedupKey` | 상태 반영                                   |
| 같은 key + 같은 payload hash    | 중복으로 무시                                 |
| 같은 key + 다른 payload hash    | anomaly 기록, 상태 반영 금지, reconciliation 후보 |

---

## 11.8.2 순서 역전 이벤트

브로커 이벤트는 순서대로 도착한다고 가정하지 않는다.

| 상황                                 | 처리                                                      |
| ---------------------------------- | ------------------------------------------------------- |
| ACK보다 Fill이 먼저 도착                  | `PENDING_ACK -> PARTIALLY_FILLED/FILLED` 허용             |
| `PENDING_CANCEL` 중 추가 partial fill | 수량 갱신, 상태는 `PENDING_CANCEL` 유지                          |
| `PENDING_CANCEL` 중 full fill       | `FILLED` 종결, active `CANCEL` instruction은 `NOT_APPLIED` |
| terminal 이후 늦은 ACK                 | 상태 변경 없음                                                |
| terminal 이후 충돌 이벤트                 | 상태 변경 금지, 운영 이력 기록 또는 reconciliation 후보                 |

---

## 11.8.3 수량 불변식 위반

다음과 같은 이벤트는 상태에 반영하지 않는다.

* `cumQty > orderQty`
* `leavesQty < 0`
* `cumQty + leavesQty > orderQty`
* `lastFillQty <= 0`
* terminal 상태와 명백히 충돌하는 체결 이벤트

처리:

1. 상태 반영 금지
2. anomaly event 기록
3. 필요 시 `reconciliationStatus = PENDING`
4. `OrderReconciliationRequested` 발행

---

## 11.9 malformed 전문 처리 정책

## 11.9.1 malformed 분류

| 유형                   | 예시                                        | 주문 식별 가능 여부 | 처리                                       |
| -------------------- | ----------------------------------------- | ----------: | ---------------------------------------- |
| Frame 오류             | length 불일치                                |           N | Gateway journal/metric 기록, 상태 변경 없음      |
| Header 오류            | `msgId`, `orderId`, `wireMessageId` 파싱 불가 |           N | Gateway journal/metric 기록, 상태 변경 없음      |
| Body 오류              | body fixed-length 파싱 실패                   |   경우에 따라 다름 | pending command와 매칭 가능하면 outcome unknown |
| Business semantic 오류 | 수량 0, 알 수 없는 상태 값                         |           Y | 상태 반영 금지, anomaly 기록                     |

---

## 11.9.2 pending command 응답 malformed

pending command와 `wireMessageId`가 매칭되지만 응답 body를 해석할 수 없는 경우, Gateway는 command 결과를 확정할 수 없다.

처리:

```text
Gateway
 -> BrokerCommandOutcomeUnknown
 -> Order Service
 -> UNKNOWN + reconciliation requested
```

이 경우 trigger type은 다음 중 하나다.

* `SUBMIT_OUTCOME_UNKNOWN`
* `CANCEL_OUTCOME_UNKNOWN`

---

## 11.9.3 식별 불가능 malformed

`orderId`를 식별할 수 없는 malformed 전문은 특정 주문에 직접 연결할 수 없다.

처리:

1. Gateway journal 기록
2. malformed metric 증가
3. 필요 시 connection close
4. Order Service 이벤트 발행 없음
5. 이후 Order Service의 stale/EOD detector가 non-terminal 주문을 간접 탐지

Phase 1에서는 `MALFORMED_SUSPECT` trigger type을 사용하지 않는다.

---

## 11.10 stale / EOD 탐지 정책

## 11.10.1 탐지 주체

stale non-terminal 주문 탐지와 EOD non-terminal DAY 주문 탐지는 **Order Service**가 수행한다.

이유:

* 주문 상태의 source of truth는 Order Service다.
* Recovery Service가 Order DB를 직접 조회하지 않아야 한다.
* Recovery Service에 별도 projection을 두는 것은 Phase 1에서 과하다.

---

## 11.10.2 stale 대상 상태

탐지 대상은 non-terminal 상태다.

* `PENDING_ACK`
* `LIVE`
* `PARTIALLY_FILLED`
* `PENDING_CANCEL`
* `UNKNOWN`

---

## 11.10.3 stale 처리

Order Service는 stale 조건에 걸린 non-terminal 주문을 reconciliation 대상으로 표시한다.

| 대상 상태              | 처리                                         |
| ------------------ | ------------------------------------------ |
| `PENDING_ACK`      | command 결과 불확실로 보고 `UNKNOWN + PENDING`     |
| `PENDING_CANCEL`   | cancel 결과 불확실로 보고 `UNKNOWN + PENDING`      |
| `LIVE`             | 기존 상태 유지, `reconciliationStatus = PENDING` |
| `PARTIALLY_FILLED` | 기존 상태 유지, `reconciliationStatus = PENDING` |
| `UNKNOWN`          | `reconciliationStatus = PENDING` 유지 또는 재요청 |

stale 감지 후 Order Service는 `OrderReconciliationRequested(triggerType=STALE_NON_TERMINAL)` 이벤트를 발행한다.

---

## 11.10.4 EOD 처리

장 마감 이후에도 DAY 주문이 terminal 상태로 수렴하지 않으면 reconciliation 대상이다.

처리:

1. 대상 주문 row lock
2. `tif = DAY` 여부 확인
3. non-terminal 상태 여부 확인
4. 기존 주문 상태 유지
5. `reconciliationStatus = PENDING`
6. `OrderReconciliationRequested(triggerType=EOD_NON_TERMINAL)` 발행

EOD sweep은 상태를 임의로 `EXPIRED`로 바꾸지 않는다.
실제 `EXPIRED` 반영은 브로커 snapshot 또는 `BrokerOrderExpired` 이벤트를 통해 Order Service 상태머신이 수행한다.

---

# 11.11 Reconciliation 재처리 정책

## 11.11.1 job 생성

Recovery Service는 `OrderReconciliationRequested` 이벤트를 수신하면 reconciliation job을 생성한다.

단, 동일 `orderId`에 active job이 있으면 새 job을 만들지 않는다.

active job 상태:

* `PENDING`
* `RUNNING`

---

## 11.11.2 attempt 생성

Recovery Service는 실행 대상 job을 claim하고 상태조회 attempt를 생성한다.

1. job 상태를 `RUNNING`으로 변경
2. attempt 생성
3. attempt 상태를 `REQUESTED`로 설정
4. `attempt_count` 증가
5. `QueryOrderStatusCommand` 발행

---

## 11.11.3 attempt 완료

Recovery Service는 Order Service의 결과 이벤트를 기준으로 attempt를 완료한다.

| 이벤트                           | attempt 처리  | job 처리                                |
| ----------------------------- | ----------- | ------------------------------------- |
| `OrderReconciliationResolved` | `RESOLVED`  | `SUCCEEDED`                           |
| `OrderReconciliationFailed`   | `FAILED`    | `FAILED`                              |
| 결과 이벤트 deadline 초과            | `TIMED_OUT` | retry 가능하면 `PENDING`, 한도 초과면 `FAILED` |

---

## 11.11.4 workflow failure

다음 경우 Recovery Service는 workflow failure로 본다.

* 상태조회 attempt 재시도 한도 초과
* Recovery 내부 오류
* 운영자 수동 중단
* 분류되지 않은 Recovery workflow 실패

이 경우 Recovery Service는 다음을 수행한다.

1. `reconciliation_job.status = FAILED`
2. `failure_type` 기록
3. `ReconciliationJobFailed` 이벤트 발행

Order Service는 이 이벤트를 받아 `reconciliationStatus = FAILED`로 반영한다.

---

## 11.11.5 domain resolution failure

상태조회 snapshot은 도착했지만 Order Service가 주문 상태로 수렴시킬 수 없는 경우다.

예:

* `snapshotStatus = NOT_FOUND`
* snapshot 수량 불변식 위반
* terminal 상태와 충돌
* 유효한 상태 전이 없음

이 경우 Order Service는 다음을 수행한다.

1. 상태를 임의로 변경하지 않는다.
2. `reconciliationStatus = FAILED`
3. `OrderReconciliationFailed` 이벤트 발행
4. 주문 이벤트 이력에 실패 사유 기록

Recovery Service는 이 이벤트를 수신해 job을 `FAILED`로 종료한다.

---

## 11.12 `NOT_FOUND` 처리 정책

브로커 상태조회 결과 `NOT_FOUND`는 자동 종결하지 않는다.

가능한 의미:

1. 실제로 브로커에 주문이 도달하지 않음
2. 조회 시점 문제
3. 브로커 쪽 상태 저장 지연
4. 식별자 문제
5. 테스트 시나리오상 일시적 불일치

기본 처리:

* Order Service는 주문을 임의로 `REJECTED`나 `EXPIRED`로 바꾸지 않는다.
* 주문의 기존 상태를 유지한다.
* `reconciliationStatus = FAILED`로 전환한다.
* active `CANCEL` instruction이 있으면 정책에 따라 `REQUESTED` 유지 또는 `FAILED` 처리한다.
* `OrderReconciliationFailed` 이벤트를 발행한다.
* 운영 추적 대상으로 남긴다.

---

## 11.13 SSE 실패 정책

SSE는 사용자 알림 수단일 뿐, 주문 상태의 source of truth가 아니다.

| 상황         | 처리                       |
| ---------- | ------------------------ |
| SSE 연결 끊김  | 클라이언트가 재연결 또는 조회 API로 보완 |
| SSE 전송 실패  | 주문 상태 rollback 없음        |
| 사용자가 늦게 연결 | 조회 API로 현재 상태 확인         |
| 이벤트 유실     | 상태 저장소는 이미 갱신되어 있어야 함    |

원칙:

> SSE 실패는 주문 상태 처리 실패가 아니다.

---

## 11.14 운영 추적 정책

모든 장애/재처리 경로는 운영자가 추적 가능해야 한다.

## 남겨야 하는 이력

| 이력                     | 위치                      |
| ---------------------- | ----------------------- |
| 주문 상태 변경               | Order event             |
| instruction 처리 상태      | Order instruction       |
| broker command attempt | Gateway command attempt |
| broker raw message     | Gateway message journal |
| malformed 전문           | Gateway message journal |
| reconciliation job     | Recovery job            |
| reconciliation attempt | Recovery attempt        |
| message publish 실패     | Outbox                  |
| message consume 중복     | Processed message       |

---

## 11.15 정책 요약

| 장애 상황                        | 처리                                                 |
| ---------------------------- | -------------------------------------------------- |
| 주문 생성 요청 중복                  | `clientOrderId` 기준 멱등 처리                           |
| 취소 요청 중복                     | `clientCancelRequestId` 기준 멱등 처리                   |
| active cancel 중복             | 충돌 처리                                              |
| 메시지 발행 실패                    | Outbox 재시도                                         |
| 메시지 재소비                      | processed message로 방어                              |
| 동일 브로커 이벤트 중복                | `brokerEventDedupKey`로 방어                          |
| submit 결과 불확실                | `UNKNOWN`, `SUBMIT_OUTCOME_UNKNOWN` reconciliation |
| cancel 결과 불확실                | `UNKNOWN`, `CANCEL_OUTCOME_UNKNOWN` reconciliation |
| pending command malformed 응답 | `BrokerCommandOutcomeUnknown`                      |
| 식별 불가능 malformed             | Gateway 기록 후 stale/EOD로 간접 복구                      |
| 순서 역전 이벤트                    | 상태머신 규칙으로 수렴                                       |
| 수량 불변식 위반                    | 상태 반영 금지, anomaly 기록, 필요 시 reconciliation          |
| `LIVE` stale                 | 상태 유지, `reconciliationStatus = PENDING`            |
| `PARTIALLY_FILLED` stale     | 상태 유지, `reconciliationStatus = PENDING`            |
| EOD non-terminal DAY         | 상태 유지, `reconciliationStatus = PENDING`            |
| 상태조회 workflow 실패             | Recovery가 `ReconciliationJobFailed` 발행             |
| snapshot 적용 실패               | Order가 `OrderReconciliationFailed` 발행              |
| `NOT_FOUND` snapshot         | 자동 종결 금지, reconciliation 실패                        |
| SSE 실패                       | 상태 처리와 분리                                          |
