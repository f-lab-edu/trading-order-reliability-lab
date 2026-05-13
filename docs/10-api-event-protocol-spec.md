# 10. API / 이벤트 / 전문 명세

## 10.1 목적

이 문서는 시스템 간 계약을 정의한다.

다루는 범위는 다음이다.

1. 사용자-facing API
2. SSE 주문 상태 알림
3. 서비스 간 command / event 메시지
4. Broker Gateway와 Broker Simulator 간 TCP 전문의 논리 명세
5. 각 계약에서 사용하는 주요 식별자와 중복 방지 기준

이 문서는 구현 코드가 아니라 **계약 명세**다.
DB 테이블명, 내부 구현 클래스명, byte-level TCP 전문 layout에 종속되지 않도록 작성한다.

TCP 전문의 byte-level layout은 이 장의 본문에서 다루지 않고, 10장 이후 별도 appendix로 작성한다.
Broker Simulator의 scenario injection API는 12장 테스트 / 모니터링 / 운영 계획에서 다룬다.

---

# 10.2 공통 표현 규칙

| 항목                | 규칙                                       |
| ----------------- | ---------------------------------------- |
| JSON field naming | `lowerCamelCase`                         |
| 시간 표현             | ISO-8601 UTC 문자열                         |
| 가격                | 문자열 decimal 권장. 예: `"189.50"`            |
| 수량                | 정수                                       |
| 내부 ID API 표현      | UUID 문자열                                 |
| 사용자 요청 멱등성        | `clientOrderId`, `clientCancelRequestId` |
| 주문 상태 기준 ID       | `orderId`                                |
| 브로커 이벤트 중복 방지     | Gateway가 부여한 `brokerEventDedupKey`       |
| 브로커 정보            | 사용자-facing API에는 기본 노출하지 않음              |
| `accountId`       | Phase 1에서는 요청 body에 포함                   |

Phase 1에서는 인증/인가 시스템을 범위에서 제외하므로 `accountId`를 요청 body에 포함한다. 실제 서비스에서는 인증 context에서 계좌 범위를 식별하는 방식으로 대체할 수 있다.

---

# 10.3 사용자-facing API

## 10.3.1 API 목록

| Method | Path                                  | 목적           |
| ------ | ------------------------------------- | ------------ |
| `POST` | `/api/orders`                         | 신규 주문 생성     |
| `GET`  | `/api/orders/{orderId}`               | 주문 상세 조회     |
| `GET`  | `/api/orders`                         | 주문 목록 조회     |
| `POST` | `/api/orders/{orderId}/cancellations` | 주문 취소 요청     |
| `GET`  | `/api/orders/stream`                  | 주문 상태 SSE 구독 |

Order Service만 사용자-facing API를 제공한다.
Broker Gateway와 Recovery Service는 사용자-facing 주문 API를 제공하지 않는다.

---

## 10.3.2 신규 주문 생성 API

```http
POST /api/orders
```

### Request

```json
{
  "clientOrderId": "client-order-20260513-0001",
  "accountId": "ACC-001",
  "market": "US",
  "symbol": "AAPL",
  "side": "BUY",
  "orderType": "LIMIT",
  "tif": "DAY",
  "orderQty": 100,
  "limitPrice": "189.50"
}
```

### 필드 의미

| 필드              | 필수 | 설명                 |
| --------------- | -: | ------------------ |
| `clientOrderId` |  Y | 주문 생성 멱등성 키        |
| `accountId`     |  Y | 계좌 또는 사용자 식별자      |
| `market`        |  Y | Phase 1에서는 `US`    |
| `symbol`        |  Y | 종목 코드              |
| `side`          |  Y | `BUY`, `SELL`      |
| `orderType`     |  Y | Phase 1에서는 `LIMIT` |
| `tif`           |  Y | Phase 1에서는 `DAY`   |
| `orderQty`      |  Y | 주문 수량              |
| `limitPrice`    |  Y | 지정가                |

### Response

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "clientOrderId": "client-order-20260513-0001",
  "status": "PENDING_ACK",
  "reconciliationStatus": "NONE",
  "orderQty": 100,
  "cumQty": 0,
  "leavesQty": 100,
  "createdAt": "2026-05-13T01:15:30.123Z"
}
```

### 처리 규칙

| 상황                                          | 처리                |
| ------------------------------------------- | ----------------- |
| 동일 `accountId + clientOrderId` + 동일 payload | 기존 주문 생성 결과 반환    |
| 동일 `accountId + clientOrderId` + 다른 payload | `409 Conflict`    |
| 시장 `CLOSED`                                 | Phase 1에서는 요청 거절  |
| 수량/가격 유효하지 않음                               | `400 Bad Request` |

---

## 10.3.3 주문 상세 조회 API

```http
GET /api/orders/{orderId}
```

### Response

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "symbol": "AAPL",
  "side": "BUY",
  "orderType": "LIMIT",
  "tif": "DAY",
  "orderQty": 100,
  "limitPrice": "189.50",
  "status": "PARTIALLY_FILLED",
  "reconciliationStatus": "NONE",
  "cumQty": 40,
  "leavesQty": 60,
  "cancelPending": false,
  "createdAt": "2026-05-13T01:15:30.123Z",
  "updatedAt": "2026-05-13T01:16:02.551Z",
  "terminalAt": null
}
```

### 원칙

사용자-facing 조회에는 브로커 코드, 브로커 주문 ID, 전문 송수신 상세를 기본 포함하지 않는다.
그 정보는 운영 추적 영역의 관심사다.

---

## 10.3.4 주문 목록 조회 API

```http
GET /api/orders?accountId=ACC-001&status=LIVE
```

### Query Parameter

| 파라미터        | 설명            |
| ----------- | ------------- |
| `accountId` | 계좌 또는 사용자 식별자 |
| `status`    | 주문 상태 필터      |
| `from`      | 생성 시각 시작      |
| `to`        | 생성 시각 종료      |
| `limit`     | 조회 개수         |
| `cursor`    | 페이지네이션 cursor |

---

## 10.3.5 주문 취소 요청 API

```http
POST /api/orders/{orderId}/cancellations
```

### Request

```json
{
  "clientCancelRequestId": "cancel-20260513-0001"
}
```

### Response

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "clientCancelRequestId": "cancel-20260513-0001",
  "orderStatus": "PENDING_CANCEL",
  "cancelStatus": "REQUESTED",
  "requestedAt": "2026-05-13T01:17:10.330Z"
}
```

### `instructionId` 노출 여부

사용자-facing API에는 내부 `instructionId`를 노출하지 않는다.

사용자는 다음 값만으로 자신의 취소 요청을 식별할 수 있다.

```text
orderId + clientCancelRequestId
```

`instructionId`는 내부 처리, 운영 추적, 디버깅 용도로만 사용한다.
운영자용 API에서는 필요 시 노출할 수 있다.

### 처리 규칙

| 상황                                                          | 처리                                              |
| ----------------------------------------------------------- | ----------------------------------------------- |
| 취소 가능 상태                                                    | `CANCEL` instruction 생성, 주문 `PENDING_CANCEL` 전환 |
| 동일 `clientCancelRequestId` 재요청                              | 기존 취소 요청 결과 반환                                  |
| 다른 `clientCancelRequestId`이나 active `CANCEL` instruction 존재 | `409 Conflict`                                  |
| 주문 `UNKNOWN`                                                | 취소 요청 거절                                        |
| 주문 terminal 상태                                              | 취소 요청 거절                                        |

---

# 10.4 SSE 주문 상태 알림

## 10.4.1 Endpoint

```http
GET /api/orders/stream
```

## 10.4.2 SSE Event 예시

```text
event: order-status-changed
id: 018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa:12
data: {
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "previousStatus": "LIVE",
  "currentStatus": "PARTIALLY_FILLED",
  "cumQty": 40,
  "leavesQty": 60,
  "occurredAt": "2026-05-13T01:18:00.100Z"
}
```

## 10.4.3 전송 대상

* 주문 접수
* 주문 거절
* 부분체결
* 완전체결
* 취소 대기
* 취소 완료
* 만료
* `UNKNOWN` 진입
* reconciliation resolved / failed

---

# 10.5 내부 메시지 공통 Envelope

서비스 간 메시지는 공통 envelope를 가진다.

```json
{
  "messageId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "messageType": "SubmitOrderCommand",
  "messageKey": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "occurredAt": "2026-05-13T01:15:30.123Z",
  "traceId": "trace-001",
  "payload": {}
}
```

| 필드            | 설명                                    |
| ------------- | ------------------------------------- |
| `messageId`   | message envelope 식별자                  |
| `messageType` | command/event 타입                      |
| `messageKey`  | Kafka key. 주문 관련 메시지는 기본적으로 `orderId` |
| `occurredAt`  | 메시지 생성 시각                             |
| `traceId`     | end-to-end 추적 ID                      |
| `payload`     | 메시지별 payload                          |

---

# 10.6 Kafka Topic

## 10.6.1 Topic 목록

| Topic                        | Producer                         | Consumer                         | 주요 메시지                                                                                                                                   |
| ---------------------------- | -------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `trading.broker.command.v1`  | Order Service / Recovery Service | Broker Gateway                   | `SubmitOrderCommand`, `CancelOrderCommand`, `QueryOrderStatusCommand`                                                                    |
| `trading.broker.event.v1`    | Broker Gateway                   | Order Service                    | `BrokerOrderAcknowledged`, `BrokerOrderFilled`, `BrokerCommandOutcomeUnknown`, `BrokerOrderStatusSnapshot` 등                             |
| `trading.order.lifecycle.v1` | Order Service                    | Recovery Service / Observability | `OrderStatusChanged`, `OrderBecameUnknown`, `OrderReconciliationRequested`, `OrderReconciliationResolved`, `OrderReconciliationFailed` 등 |

## 10.6.2 Topic Naming 기준

초기 topic은 도메인 prefix + 데이터 성격 + version을 사용한다.

```text
trading.<domain>.<message-category>.v1
```

예:

```text
trading.broker.command.v1
trading.broker.event.v1
trading.order.lifecycle.v1
```

원칙:

* 환경명은 topic 이름에 넣지 않는다.
* producer 서비스명을 topic 이름에 직접 박지 않는다.
* message type은 envelope의 `messageType`으로 구분한다.
* topic은 너무 세분화하지 않는다.
* version suffix는 유지한다.

---

# 10.7 Broker Command 명세

## 10.7.1 `SubmitOrderCommand`

Producer: Order Service
Consumer: Broker Gateway
Topic: `trading.broker.command.v1`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "accountId": "ACC-001",
  "market": "US",
  "symbol": "AAPL",
  "side": "BUY",
  "orderType": "LIMIT",
  "tif": "DAY",
  "orderQty": 100,
  "limitPrice": "189.50"
}
```

---

## 10.7.2 `CancelOrderCommand`

Producer: Order Service
Consumer: Broker Gateway
Topic: `trading.broker.command.v1`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa"
}
```

Gateway는 `orderId` 기준으로 브로커 binding을 조회하고 취소 전문을 생성한다.
Order Service는 브로커 주문 ID를 알 필요가 없다.

---

## 10.7.3 `QueryOrderStatusCommand`

Producer: Recovery Service
Consumer: Broker Gateway
Topic: `trading.broker.command.v1`

```json
{
  "jobId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "attemptId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ab",
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ac",
  "triggerType": "CANCEL_OUTCOME_UNKNOWN"
}
```

---

# 10.8 Canonical Broker Event 명세

Broker Gateway는 브로커 전문을 Order Service가 처리 가능한 canonical event로 변환한다.

## 10.8.1 공통 필드

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "brokerEventTime": "2026-05-13T01:15:35.500Z"
}
```

| 필드                    | 설명                           |
| --------------------- | ---------------------------- |
| `orderId`             | 상태를 적용할 주문 ID                |
| `brokerEventDedupKey` | 동일 외부 브로커 사건 중복 방지 key       |
| `payloadHash`         | 동일 key의 payload mismatch 탐지용 |
| `brokerEventTime`     | 브로커 사건 발생 시각                 |

Order Service는 `brokerEventDedupKey`를 opaque value로 취급한다.

---

## 10.8.2 이벤트 목록

| Event                         | 의미             |
| ----------------------------- | -------------- |
| `BrokerOrderAcknowledged`     | 주문 접수          |
| `BrokerOrderRejected`         | 주문 거절          |
| `BrokerOrderPartiallyFilled`  | 부분체결           |
| `BrokerOrderFilled`           | 완전체결           |
| `BrokerCancelAcknowledged`    | 취소 완료          |
| `BrokerCancelRejected`        | 취소 거절          |
| `BrokerOrderExpired`          | DAY 주문 만료      |
| `BrokerOrderStatusSnapshot`   | 상태조회 snapshot  |
| `BrokerCommandOutcomeUnknown` | command 결과 불확실 |

---

## 10.8.3 `BrokerOrderAcknowledged`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "brokerEventTime": "2026-05-13T01:15:35.500Z"
}
```

---

## 10.8.4 `BrokerOrderRejected`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "rejectCode": "INVALID_PRICE",
  "rejectMessage": "Invalid limit price",
  "brokerEventTime": "2026-05-13T01:15:35.500Z"
}
```

---

## 10.8.5 `BrokerOrderPartiallyFilled`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "lastFillQty": 40,
  "cumQty": 40,
  "leavesQty": 60,
  "brokerEventTime": "2026-05-13T01:18:00.100Z"
}
```

---

## 10.8.6 `BrokerOrderFilled`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "lastFillQty": 60,
  "cumQty": 100,
  "leavesQty": 0,
  "brokerEventTime": "2026-05-13T01:20:00.100Z"
}
```

---

## 10.8.7 `BrokerCancelAcknowledged`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "brokerEventTime": "2026-05-13T01:21:00.100Z"
}
```

---

## 10.8.8 `BrokerCancelRejected`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "rejectCode": "TOO_LATE_TO_CANCEL",
  "rejectMessage": "Order is already filled",
  "brokerEventTime": "2026-05-13T01:21:00.100Z"
}
```

---

## 10.8.9 `BrokerOrderExpired`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "cumQty": 40,
  "leavesQty": 0,
  "brokerEventTime": "2026-05-13T20:00:00.000Z"
}
```

---

## 10.8.10 `BrokerOrderStatusSnapshot`

```json
{
  "jobId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "attemptId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ab",
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ac",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "snapshotStatus": "PARTIAL",
  "cumQty": 40,
  "leavesQty": 60,
  "brokerEventTime": "2026-05-13T01:20:00.000Z"
}
```

`snapshotStatus` 값:

* `ACCEPTED`
* `PARTIAL`
* `FILLED`
* `CANCELED`
* `REJECTED`
* `EXPIRED`
* `NOT_FOUND`

---

## 10.8.11 `BrokerCommandOutcomeUnknown`

`BrokerCommandOutcomeUnknown`은 malformed 일반 이벤트가 아니다.

정확한 의미는 다음이다.

> Broker Gateway가 브로커 command를 보냈지만, 해당 command의 업무 결과를 확정할 수 없을 때 Order Service에 전달하는 canonical broker event.

### 발생 예시

| 상황                                         | 발행 여부                     |
| ------------------------------------------ | ------------------------- |
| `ORDR` 전송 후 ACK/Reject timeout             | 발행                        |
| `CXLQ` 전송 후 CancelAck/CancelReject timeout | 발행                        |
| pending command 응답으로 보이나 body 파싱 실패        | 발행 가능                     |
| `orderId`도 못 읽는 malformed 전문               | 발행하지 않음                   |
| 비동기 FILL 전문 malformed이며 주문 식별 불가           | 발행하지 않음                   |
| 명시적 주문 거절                                  | `BrokerOrderRejected` 사용  |
| 명시적 취소 거절                                  | `BrokerCancelRejected` 사용 |

### Payload

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "brokerEventDedupKey": "opaque-dedup-key",
  "payloadHash": "sha256-hash",
  "commandType": "CANCEL",
  "unknownReason": "TIMEOUT",
  "brokerEventTime": "2026-05-13T01:22:00.000Z"
}
```

`commandType` 값:

* `SUBMIT`
* `CANCEL`
* `QUERY_STATUS`

`unknownReason` 값:

* `TIMEOUT`
* `CONNECTION_CLOSED`
* `MALFORMED_RESPONSE`
* `UNKNOWN_TRANSPORT_ERROR`

### 처리 흐름

```text
Broker Gateway
  -> BrokerCommandOutcomeUnknown
  -> Order Service
  -> OrderStatusChanged(currentStatus = UNKNOWN)
  -> OrderReconciliationRequested(triggerType = SUBMIT_OUTCOME_UNKNOWN or CANCEL_OUTCOME_UNKNOWN)
  -> Recovery Service
```

`BrokerCommandOutcomeUnknown`은 `trading.broker.event.v1` topic으로 전달한다.

---

# 10.9 Order Lifecycle Event 명세

Order Service는 주문 상태 변화와 reconciliation 요청/결과를 lifecycle event로 발행한다.

## 10.9.1 이벤트 목록

| Event                          | Consumer                 | 목적                       |
| ------------------------------ | ------------------------ | ------------------------ |
| `OrderCreated`                 | Observability            | 주문 생성 관측                 |
| `OrderStatusChanged`           | SSE / Observability      | 상태 변경 알림                 |
| `OrderBecameUnknown`           | Recovery / Observability | `UNKNOWN` 진입 알림          |
| `OrderReconciliationRequested` | Recovery                 | reconciliation job 생성 요청 |
| `OrderReconciliationResolved`  | Recovery / Observability | reconciliation 성공 알림     |
| `OrderReconciliationFailed`    | Recovery / Observability | reconciliation 실패 알림     |
| `OrderTerminalized`            | Observability            | 주문 종결 알림                 |

`OrderBecameUnknown`과 `OrderReconciliationRequested`는 합치지 않는다.

| 이벤트                            | 의미                                                      |
| ------------------------------ | ------------------------------------------------------- |
| `OrderBecameUnknown`           | 주문 상태가 `UNKNOWN`으로 변경되었다는 상태 변화 사실                      |
| `OrderReconciliationRequested` | Recovery Service에게 reconciliation workflow 생성을 요청하는 이벤트 |

`UNKNOWN` 상태가 아닌 주문도 stale/EOD 정책에 의해 reconciliation 대상이 될 수 있으므로 두 이벤트를 분리한다.

---

## 10.9.2 `OrderStatusChanged`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "previousStatus": "LIVE",
  "currentStatus": "PARTIALLY_FILLED",
  "cumQty": 40,
  "leavesQty": 60,
  "changedAt": "2026-05-13T01:18:00.100Z"
}
```

---

## 10.9.3 `OrderBecameUnknown`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "previousStatus": "PENDING_CANCEL",
  "currentStatus": "UNKNOWN",
  "unknownReason": "CANCEL_OUTCOME_UNKNOWN",
  "changedAt": "2026-05-13T01:21:00.000Z"
}
```

---

## 10.9.4 `OrderReconciliationRequested`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "triggerType": "CANCEL_OUTCOME_UNKNOWN",
  "currentStatus": "UNKNOWN",
  "reconciliationStatus": "PENDING",
  "requestedAt": "2026-05-13T01:21:00.000Z"
}
```

`triggerType` 값:

* `SUBMIT_OUTCOME_UNKNOWN`
* `CANCEL_OUTCOME_UNKNOWN`
* `STALE_NON_TERMINAL`
* `EOD_NON_TERMINAL`
* `MANUAL`

`MALFORMED_SUSPECT`는 Phase 1 trigger type에서 제외한다.

---

## 10.9.5 `OrderReconciliationResolved`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "jobId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ab",
  "attemptId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ac",
  "resolvedStatus": "CANCELED",
  "resolvedAt": "2026-05-13T01:22:00.000Z"
}
```

---

## 10.9.6 `OrderReconciliationFailed`

```json
{
  "orderId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa",
  "jobId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ab",
  "attemptId": "018f8b7a-4c4e-7b20-9f0e-9dfeb33e92ac",
  "currentStatus": "UNKNOWN",
  "failedAt": "2026-05-13T01:25:00.000Z"
}
```

---

# 10.10 Broker TCP 전문 논리 명세

이 단계에서는 byte offset을 확정하지 않는다.
byte-level layout은 이 장 이후 별도 appendix로 작성한다.

## 10.10.1 Frame 구조

```text
[length header][common header][fixed-length body]
```

| 영역                  | 설명                |
| ------------------- | ----------------- |
| `length header`     | 전체 전문 길이          |
| `common header`     | 전문 공통 메타데이터       |
| `fixed-length body` | 전문 ID별 고정 길이 body |

---

## 10.10.2 Common Header 논리 필드

| 필드              | 설명                           |
| --------------- | ---------------------------- |
| `msgId`         | 전문 ID                        |
| `wireMessageId` | 전문 단위 ID                     |
| `orderId`       | 내부 주문 참조값. Phase 1에서는 전문에 포함 |
| `traceId`       | 추적 ID                        |
| `sentAt`        | 전문 생성 시각                     |

---

## 10.10.3 전문 ID 목록

| msgId  | 방향               | 의미         |
| ------ | ---------------- | ---------- |
| `ORDR` | Gateway → Broker | 주문 요청      |
| `ACKN` | Broker → Gateway | 주문 접수      |
| `RJCT` | Broker → Gateway | 주문 거절      |
| `FILL` | Broker → Gateway | 체결         |
| `CXLQ` | Gateway → Broker | 취소 요청      |
| `CXLA` | Broker → Gateway | 취소 완료      |
| `CXLR` | Broker → Gateway | 취소 거절      |
| `EXPR` | Broker → Gateway | DAY 주문 만료  |
| `OSTQ` | Gateway → Broker | 주문 상태조회    |
| `OSTS` | Broker → Gateway | 주문 상태조회 응답 |

---

## 10.10.4 전문 설계 원칙

* Gateway와 Broker Simulator만 전문 포맷을 안다.
* Order Service는 전문 포맷을 모른다.
* 전문 payload는 fixed-length field로 구성한다.
* 문자열 필드는 right-padding한다.
* 숫자 필드는 zero-padding한다.
* malformed 전문은 Gateway에서 분류한다.
* 식별 불가능한 malformed 전문은 주문 상태를 직접 변경하지 않는다.
* pending command 결과를 확정할 수 없는 malformed 응답은 `BrokerCommandOutcomeUnknown`으로 변환할 수 있다.
* 비동기 이벤트 malformed이며 주문 식별이 불가능한 경우 journal/metric만 남기고 stale/EOD 탐지로 간접 복구한다.
* Broker Gateway는 정상 또는 해석 가능한 전문을 canonical broker event로 변환한다.

---

# 10.11 Malformed 처리 기준

`MALFORMED_SUSPECT` trigger type은 Phase 1에서 사용하지 않는다.

Malformed는 다음 두 경로로 처리한다.

## 10.11.1 Pending command 결과 불확실성

예:

* `ORDR` 응답으로 보이지만 body 파싱 실패
* `CXLQ` 응답으로 보이지만 cancel 결과 해석 불가
* pending command와 `wireMessageId`가 매칭되지만 업무 결과를 확정할 수 없음

처리:

```text
Gateway
  -> BrokerCommandOutcomeUnknown
  -> Order Service
  -> OrderReconciliationRequested(
       triggerType = SUBMIT_OUTCOME_UNKNOWN
       or CANCEL_OUTCOME_UNKNOWN
     )
```

## 10.11.2 식별 불가능 malformed

예:

* length header 불일치
* header 파싱 실패
* `orderId` 식별 불가
* 비동기 FILL/EXPR 전문 malformed로 주문 귀속 불가

처리:

```text
Gateway
  -> journal / metric 기록
  -> Order Service 이벤트 없음
  -> 이후 Order Service stale/EOD detector가 간접 탐지
  -> OrderReconciliationRequested(
       triggerType = STALE_NON_TERMINAL
       or EOD_NON_TERMINAL
     )
```
