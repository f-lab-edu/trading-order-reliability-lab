# 3. 핵심 요구사항 / 비기능 요구사항

## 3.1 요구사항 정의 원칙

본 시스템의 핵심 가치는 **해외주식 주문 상태 가시성**과 **불확실한 외부 브로커 응답에 대한 정합성 복구**다.

요구사항은 다음 원칙을 따른다.

1. 외부 브로커 응답은 지연, 유실, 중복, 순서 역전될 수 있다고 가정한다.
2. 주문 상태의 최종 판단은 내부 Order Service가 수행한다.
3. 애매한 상태는 성공/실패로 단정하지 않고 `UNKNOWN`으로 표현한다.
4. `UNKNOWN` 상태는 reconciliation을 통해 최종 주문 상태로 수렴해야 한다.
5. TCP 전문 통신은 실제 금융권 대외연계의 핵심 구조만 단순화해 재현한다.
6. 1차 범위는 단일 브로커 기준으로 구현한다.
7. 심화 확장은 **멀티 브로커 라우팅 / fallback → 운영 콘솔** 순서로 진행한다.

---

## 3.2 핵심 식별자 정의

본 시스템에서는 여러 식별자가 함께 사용된다.
각 식별자는 모두 “주문을 찾기 위한 값”처럼 보일 수 있지만, 생성 주체와 책임 경계가 다르다.

핵심 원칙은 다음과 같다.

> `orderId`는 우리 시스템 내부의 주문 식별자다.
> `clientOrderId`와 `clientCancelRequestId`는 클라이언트 요청 재시도를 안전하게 처리하기 위한 멱등성 키다.
> `wireMessageId`는 외부 브로커 전문 단위의 식별자다.
> `brokerOrderId`는 브로커가 부여하는 외부 주문 식별자이며, 주문 도메인 상태 판단의 기준으로 사용하지 않는다.
> `traceId`는 상태 판단이나 멱등성이 아니라 관측성을 위한 추적 ID다.

---

### 3.2.1 식별자 요약

| 식별자                     | 생성 주체                              | 생성 시점                            | 사용 경계                             | 목적                                     |
| ----------------------- | ---------------------------------- | -------------------------------- | --------------------------------- | -------------------------------------- |
| `orderId`               | Order Service                      | 주문 생성 요청이 유효하게 접수되어 내부 주문이 생성될 때 | 우리 시스템 내부                         | 주문 aggregate 식별, 상태 변경 기준              |
| `clientOrderId`         | Client                             | 주문 생성 API 호출 전                   | Client ↔ Order Service            | 주문 생성 요청 멱등성                           |
| `clientCancelRequestId` | Client                             | 주문 취소 API 호출 전                   | Client ↔ Order Service            | 취소 요청 멱등성                              |
| `wireMessageId`         | Broker Gateway 또는 Broker Simulator | TCP 전문 생성 시                      | Broker Gateway ↔ Broker Simulator | 전문 단위 추적, 요청/응답 correlation, 외부 이벤트 식별 |
| `brokerEventDedupKey`   | Broker Gateway                     | 브로커 전문을 canonical event로 변환할 때   | Broker Gateway → Order Service    | 동일 외부 브로커 사건의 중복 반영 방지                 |
| `traceId`               | Client 또는 최초 진입 서비스                | 하나의 업무 처리 흐름이 시작될 때              | 전체 시스템                            | end-to-end 관측성 추적                      |
| `brokerOrderId`         | Broker Simulator                   | 브로커가 주문을 접수할 때                   | Broker Gateway ↔ Broker Simulator | 브로커 측 주문 식별, 운영 추적, 상태조회 보조            |

---

### 3.2.2 `orderId`

`orderId`는 우리 시스템 내부의 주문 식별자다.

사용자가 주문 생성 요청을 보내고, 시스템이 이를 유효한 주문으로 받아들여 내부 주문을 생성할 때 Order Service가 부여한다.

`orderId`는 다음 용도로 사용한다.

* 주문 상세 조회
* 주문 취소 요청 대상 식별
* 주문 상태 변경 기준
* 주문 이벤트 귀속
* 내부 메시지의 주문 기준 key
* reconciliation 대상 주문 식별
* canonical broker event가 어떤 주문에 적용되어야 하는지 나타내는 기준

`orderId`는 클라이언트가 최초 주문 요청 전에 알 수 없다.
따라서 주문 생성 API의 멱등성 키로 사용할 수 없다.

---

### 3.2.3 `clientOrderId`

`clientOrderId`는 클라이언트가 주문 생성 요청 전에 생성하는 멱등성 키다.

필요한 이유는 다음과 같다.

```text
1. Client가 주문 생성 요청을 보낸다.
2. Order Service는 주문을 생성하고 orderId를 부여한다.
3. 그런데 응답이 Client에게 도달하기 전에 네트워크 장애가 발생한다.
4. Client는 주문이 생성되었는지 알 수 없다.
5. Client가 같은 주문 생성 요청을 재시도한다.
```

이때 클라이언트는 아직 `orderId`를 모를 수 있다.
따라서 클라이언트가 미리 생성한 `clientOrderId`로 동일 주문 생성 요청인지 판단해야 한다.

고유성은 다음 기준으로 본다.

```text
accountId + clientOrderId
```

같은 `accountId + clientOrderId`로 동일 payload가 다시 들어오면 기존 주문 결과를 반환한다.
같은 `accountId + clientOrderId`인데 payload가 다르면 충돌로 처리한다.

---

### 3.2.4 `clientCancelRequestId`

`clientCancelRequestId`는 클라이언트가 취소 요청 전에 생성하는 멱등성 키다.

취소 요청도 주문 생성과 마찬가지로 네트워크 장애나 클라이언트 재시도로 중복 호출될 수 있다.

예를 들어:

```text
1. Client가 주문 취소 요청을 보낸다.
2. 시스템은 취소 요청을 접수하고 브로커 취소 처리를 시작한다.
3. 응답이 Client에게 도달하기 전에 네트워크 장애가 발생한다.
4. Client가 같은 취소 요청을 재시도한다.
```

이때 같은 취소 요청을 중복 생성하거나, 브로커에 중복 취소 전문을 보내면 안 된다.

API 의미상 취소 요청은 path의 `orderId`로 대상 주문을 지정하고, `clientCancelRequestId`로 같은 취소 요청의 재시도를 식별한다.

내부 멱등성 고유성은 주문 생성과 같은 instruction 모델로 일반화해 다음 기준으로 본다.

```text
accountId + instructionType + clientInstructionId
```

이때 취소 요청은 다음처럼 해석한다.

```text
instructionType = CANCEL
clientInstructionId = clientCancelRequestId
```

`orderId`는 unique key 자체에는 포함하지 않지만, 취소 대상 주문을 식별하고 같은 `clientCancelRequestId`가 동일 주문에 대한 재시도인지 검증하는 payload hash에 포함한다.

정리하면:

* 동일 `accountId + CANCEL + clientCancelRequestId`와 동일 `orderId` 및 동일 payload가 다시 들어오면 기존 취소 instruction 결과를 반환한다.
* 동일 `accountId + CANCEL + clientCancelRequestId`지만 `orderId` 또는 payload가 다르면 충돌로 처리한다.
* 동일 주문에 다른 `clientCancelRequestId`가 들어왔더라도 active `CANCEL` instruction이 있으면 충돌로 처리한다.

이 일반화는 내부 설계 개념이며, API에서는 사용자 의미에 맞게 `clientOrderId`, `clientCancelRequestId`라는 이름을 유지한다.

---

### 3.2.5 `clientOrderId`, `clientCancelRequestId`, `orderId`의 관계

주문 생성 흐름에서의 관계는 다음과 같다.

```text
Client
  clientOrderId 생성
      ↓
Order Service
  주문 생성 요청 멱등성 확인
      ↓
  orderId 생성
      ↓
  주문 상태를 PENDING_ACK로 시작
```

취소 흐름에서의 관계는 다음과 같다.

```text
Client
  clientCancelRequestId 생성
      ↓
Order Service
  orderId 기준 주문 조회
      ↓
  취소 요청 멱등성 확인
      ↓
  주문 상태를 PENDING_CANCEL로 전환
```

정리하면 다음과 같다.

| 항목                  | `clientOrderId`        | `clientCancelRequestId` | `orderId`       |
| ------------------- | ---------------------- | ----------------------- | --------------- |
| 생성 주체               | Client                 | Client                  | Order Service   |
| 목적                  | 주문 생성 멱등성              | 취소 요청 멱등성               | 주문 aggregate 식별 |
| 최초 요청 전 Client가 아는가 | 예                      | 예                       | 아니오             |
| 주문 상태 변경 기준인가       | 아니오                    | 아니오                     | 예               |
| 사용 범위               | Client ↔ Order Service | Client ↔ Order Service  | 시스템 내부 전반       |

---

### 3.2.6 `wireMessageId`

`wireMessageId`는 TCP 전문 단위의 식별자다.

전문을 보내는 쪽이 생성한다.

| 상황                                 | 생성 주체            |
| ---------------------------------- | ---------------- |
| Gateway가 브로커로 요청 전문을 보낼 때          | Broker Gateway   |
| Broker Simulator가 비동기 이벤트 전문을 보낼 때 | Broker Simulator |

요청/응답 관계에서는 브로커가 동일 `wireMessageId`를 응답 전문에 echo한다.

```text
ORDR wireMessageId = W-GW-001
ACKN wireMessageId = W-GW-001
```

브로커가 비동기로 발생시키는 체결/만료 이벤트는 Broker Simulator가 새 `wireMessageId`를 생성한다.

```text
FILL wireMessageId = W-BRK-101
EXPR wireMessageId = W-BRK-201
```

동일 논리 이벤트를 중복 전송할 경우에는 동일 `wireMessageId`를 재사용한다.

`wireMessageId`는 다음 용도로 사용한다.

* 전문 송수신 추적
* 요청/응답 correlation
* command attempt timeout 분석
* 브로커 이벤트 중복 판단을 위한 재료
* 운영 디버깅

단, Order Service는 `wireMessageId`를 직접 해석하지 않는다.
전문 계층의 세부 정보는 Broker Gateway가 처리한다.

---

### 3.2.7 `brokerEventDedupKey`

`brokerEventDedupKey`는 Broker Gateway가 브로커 전문을 canonical broker event로 변환할 때 부여하는 중복 방지 키다.

목적은 다음이다.

> 서로 다른 메시지에 담겨 들어온 같은 외부 브로커 사건이 주문 상태에 중복 반영되지 않도록 한다.

예를 들어 브로커가 동일 체결 전문을 두 번 보낼 수 있다.

```text
FILL wireMessageId = W-BRK-101
FILL wireMessageId = W-BRK-101
```

Gateway는 두 번 모두 canonical event로 변환할 수 있다.
이때 두 canonical event는 서로 다른 메시지 ID를 가질 수 있지만, 같은 외부 사건을 의미한다.

```text
messageId = M-001, brokerEventDedupKey = D-101
messageId = M-002, brokerEventDedupKey = D-101
```

Order Service는 `brokerEventDedupKey`를 기준으로 이미 반영한 외부 사건인지 판단한다.

중요한 원칙은 다음이다.

> Order Service는 `brokerEventDedupKey`의 내부 구조를 해석하지 않는다.
> 동일한 `brokerEventDedupKey`는 동일한 외부 브로커 사건이라는 계약만 사용한다.

Phase 1에서 Broker Gateway는 내부적으로 다음과 같은 조합으로 dedup key를 만들 수 있다.

```text
brokerCode + ":" + msgId + ":" + wireMessageId
```

하지만 이 조합 방식은 Gateway 내부 구현 규칙이다.
Order Service 요구사항에서는 opaque key로 취급한다.

같은 `brokerEventDedupKey`가 다시 들어오면 중복 이벤트로 보고 주문 상태에 재반영하지 않는다.
같은 `brokerEventDedupKey`인데 payload가 다르면 브로커 오류 또는 프로토콜 위반으로 보고 상태에 반영하지 않는다. 이 경우 필요 시 reconciliation 후보로 올린다.

---

### 3.2.8 `brokerOrderId`

`brokerOrderId`는 브로커가 주문을 접수하면서 부여하는 브로커 측 주문 식별자다.

생성 흐름은 다음과 같다.

```text
Broker Gateway
  ORDR 전문 전송
      ↓
Broker Simulator
  주문 접수
  brokerOrderId 생성
      ↓
Broker Gateway
  ACKN 전문 수신
```

`brokerOrderId`는 다음 용도로 사용한다.

* Broker Gateway의 브로커 전문 추적
* 브로커 상태조회 보조
* 운영자가 브로커 관점에서 주문을 추적할 때 사용
* 향후 멀티 브로커 확장 시 외부 주문 식별

중요한 점은 다음이다.

> `brokerOrderId`는 Order Service의 주문 상태 판단 기준이 아니다.
> Order Service는 `orderId`를 기준으로 주문 상태를 변경한다.
> 브로커 선택, 브로커 주문 ID, 전문 송수신은 Broker Gateway의 책임이다.

사용자-facing 주문 조회에서는 일반적으로 `brokerOrderId`를 노출하지 않는다.
운영 추적이나 Gateway 내부 분석에서 사용한다.

---

### 3.2.9 `traceId`

`traceId`는 하나의 업무 처리 흐름을 추적하기 위한 관측성 ID다.

Phase 1에서는 주문 생성 trace를 후속 브로커 이벤트까지 전파한다.

```text
POST /orders traceId = T1
SubmitOrderCommand traceId = T1
ORDR traceId = T1
ACKN traceId = T1
BrokerOrderAcknowledged traceId = T1
OrderStatusChanged traceId = T1
SSE traceId = T1
```

`traceId`는 다음에 사용한다.

* 애플리케이션 로그 연결
* 메시지 흐름 추적
* 전문 송수신 이력 추적
* 주문 이벤트 이력 추적
* 장애 분석

단, `traceId`는 상태 판단이나 멱등성 판단에 사용하지 않는다.

| 사용하면 안 되는 용도  |
| ------------- |
| 주문 생성 멱등성     |
| 취소 요청 멱등성     |
| 주문 상태 판단      |
| 브로커 이벤트 중복 방지 |

---

### 3.2.10 전체 생애주기에서의 식별자 흐름

주문 생성 흐름은 다음과 같다.

```text
Client
  clientOrderId 생성
      ↓
Order Service
  PLACE instruction으로 접수
  orderId 생성
      ↓
Broker Gateway
  orderId를 포함한 주문 전문 전송
  wireMessageId 생성
      ↓
Broker Simulator
  brokerOrderId 생성
      ↓
Broker Gateway
  brokerOrderId 기록
  canonical broker event 생성
      ↓
Order Service
  orderId 기준 주문 상태 반영
```

취소 흐름은 다음과 같다.

```text
Client
  clientCancelRequestId 생성
      ↓
Order Service
  orderId 기준 주문 조회
  CANCEL instruction으로 접수
      ↓
Broker Gateway
  orderId를 포함한 취소 전문 전송
  wireMessageId 생성
      ↓
Broker Simulator
  취소 완료/거절 전문 응답
      ↓
Order Service
  orderId 기준 주문 상태 반영
```

브로커 이벤트 중복 방지는 다음 흐름으로 처리한다.

```text
Broker Simulator
  동일 외부 사건을 중복 전송
      ↓
Broker Gateway
  동일 brokerEventDedupKey를 가진 canonical event 생성
      ↓
Order Service
  brokerEventDedupKey 기준으로 이미 반영한 사건인지 확인
      ↓
  중복이면 상태 재반영 금지
```

전체 관측성은 다음으로 연결한다.

```text
traceId
```

---

### 3.2.11 한 줄 요약

| ID                      | 한 줄 정의                     |
| ----------------------- | -------------------------- |
| `orderId`               | 우리 시스템의 주문 aggregate ID    |
| `clientOrderId`         | 주문 생성 요청의 클라이언트 멱등성 키      |
| `clientCancelRequestId` | 취소 요청의 클라이언트 멱등성 키         |
| `wireMessageId`         | TCP 전문 단위 식별자              |
| `brokerEventDedupKey`   | Gateway가 생성한 외부 사건 중복 방지 키 |
| `brokerOrderId`         | 브로커가 부여한 외부 주문 ID          |
| `traceId`               | end-to-end 관측성 추적 ID       |

---

## 3.3 Scope Constraints

### 주문 제약

* 주문 타입은 `LIMIT`만 지원한다.
* TIF는 `DAY`로 고정한다.
* 시장 상태는 `OPEN`, `CLOSED`만 지원한다.
* Phase 1에서는 별도 Market Service를 두지 않는다.
* 시장 상태 `OPEN` / `CLOSED`는 Order Service가 주문 생성 검증과 EOD sweep 판단을 위해 보유하는 단순 runtime/config state로 둔다.
* 시장 상태 전환은 테스트/운영용 admin endpoint 또는 내부 설정으로 수행한다.
* 수량은 정수 수량만 지원한다.
* 1차 범위에서는 단일 브로커만 지원한다.
* Phase 1의 체결 모델은 수량 중심으로 단순화한다.
* 주문에는 `limitPrice`를 보유하지만, 체결 이벤트에서는 `lastFillPrice`, `avgFillPrice`를 다루지 않는다.

### 제외 범위

* 실제 브로커 연동
* 실제 FIX 프로토콜 구현
* 시장가 주문
* 다양한 TIF 옵션
* 장전/정규장/장후/휴장 캘린더
* 실시간 시세 시스템
* buying power
* position
* 잔고 관리
* 계좌 관리
* 환전
* 정산
* 세금
* 수수료
* 운영 콘솔 UI

### 심화 범위

1차 구현 이후 다음 순서로 확장한다.

1. 멀티 브로커 라우팅 / fallback
2. 운영 콘솔

---

# 3.4 기능 요구사항

## FR-001. 주문 생성

시스템은 사용자가 해외주식 지정가 주문을 생성할 수 있어야 한다.

### 수용 기준

* 유효한 주문 요청은 내부 주문으로 저장되어야 한다.
* 주문 생성 시 Order Service는 `orderId`를 생성해야 한다.
* 주문 생성 요청에는 `clientOrderId`가 포함되어야 한다.
* 주문 생성 직후 상태는 `PENDING_ACK`여야 한다.
* 주문 생성 후 외부 브로커 전송 요청이 생성되어야 한다.

---

## FR-002. 주문 생성 멱등성

시스템은 동일 `clientOrderId`를 가진 중복 주문 요청을 안전하게 처리해야 한다.

### 수용 기준

* 동일 `accountId + clientOrderId`와 동일 payload가 다시 들어오면 기존 주문 결과를 반환해야 한다.
* 동일 `accountId + clientOrderId`지만 payload가 다르면 충돌로 처리해야 한다.
* 중복 요청으로 내부 주문이 중복 생성되면 안 된다.
* 중복 요청으로 외부 브로커에 주문이 중복 전송되면 안 된다.

---

## FR-003. 주문 조회

시스템은 사용자가 주문 현재 상태를 조회할 수 있어야 한다.

### 조회 정보

* 주문 상태
* 종목
* 매수/매도
* 주문 수량
* 지정가
* 누적 체결 수량
* 잔여 수량
* reconciliation 상태
* 생성/수정/종결 시각
* 취소 진행 여부

브로커 코드, 브로커 주문 ID, 전문 송수신 상세는 사용자-facing 기본 조회 정보에 포함하지 않는다.
이 정보는 Broker Gateway와 운영 추적 영역에서 사용한다.

### 수용 기준

* 주문 상세 조회는 현재 주문 상태를 반환해야 한다.
* 주문 목록 조회는 사용자 또는 계좌 기준으로 필터링되어야 한다.
* 존재하지 않는 주문은 명확한 오류로 응답해야 한다.
* 다른 사용자의 주문을 조회할 수 없어야 한다.

---

## FR-004. 주문 취소

시스템은 사용자가 미종결 주문에 대해 취소를 요청할 수 있어야 한다.

### 취소 허용 상태

* `PENDING_ACK`
* `LIVE`
* `PARTIALLY_FILLED`

### 취소 거부 상태

* `UNKNOWN`
* `FILLED`
* `CANCELED`
* `REJECTED`
* `EXPIRED`

### 수용 기준

* 취소 요청이 수락되면 주문 상태는 `PENDING_CANCEL`이 되어야 한다.
* 부분체결 상태에서 취소 요청이 들어오면 이미 체결된 수량은 유지하고 미체결 잔량만 취소 대상으로 삼아야 한다.
* 취소 요청 이후 브로커 응답에 따라 `CANCELED`, `FILLED`, `PARTIALLY_FILLED`, `UNKNOWN` 등으로 수렴할 수 있어야 한다.
* 동일 취소 요청이 중복으로 들어와도 중복 취소 전문이 전송되면 안 된다.
* 취소 요청은 주문이 속한 account/user 범위 안에서만 허용되어야 한다.

---

## FR-005. 주문 상태 전이 처리

시스템은 외부 브로커 이벤트를 기반으로 주문 상태를 올바르게 전이해야 한다.

### 처리 대상 이벤트

* 주문 접수
* 주문 거절
* 부분체결
* 완전체결
* 취소 완료
* 취소 거절
* DAY 주문 만료
* 상태조회 결과

### 수용 기준

* 주문 접수 이벤트는 주문을 `LIVE`로 전환해야 한다.
* 주문 거절 이벤트는 주문을 `REJECTED`로 종결해야 한다.
* 부분체결 이벤트는 주문을 `PARTIALLY_FILLED`로 전환하고 누적 체결 수량과 잔여 수량을 갱신해야 한다.
* 완전체결 이벤트는 주문을 `FILLED`로 종결해야 한다.
* 취소 완료 이벤트는 주문을 `CANCELED`로 종결해야 한다.
* 장 마감 만료 이벤트는 주문을 `EXPIRED`로 종결해야 한다.
* 종결 상태 이후 잘못 도착한 이벤트는 주문 상태를 오염시키면 안 된다.

---

## FR-006. 부분체결 처리

시스템은 주문의 부분체결을 지원해야 한다.

### 수용 기준

* 부분체결 시 `cumQty`와 `leavesQty`가 갱신되어야 한다.
* `0 < cumQty < orderQty`인 경우 주문 상태는 `PARTIALLY_FILLED`여야 한다.
* 동일 체결 이벤트가 중복 수신되어도 누적 체결 수량이 중복 반영되면 안 된다.
* 최종적으로 `cumQty == orderQty`가 되면 주문은 `FILLED`로 종결되어야 한다.

---

## FR-007. 부분체결 후 취소 처리

시스템은 부분체결된 주문의 미체결 잔량에 대해 취소 요청을 처리할 수 있어야 한다.

부분체결 이후 취소 요청은 이미 체결된 수량을 취소하지 않는다.
취소 요청은 미체결 잔량에 대해서만 적용된다.

### 수용 기준

* `PARTIALLY_FILLED` 상태에서 취소 요청이 가능해야 한다.
* 취소 요청이 수락되면 주문은 `PENDING_CANCEL`로 전환되어야 한다.
* 취소 완료 시 주문은 `CANCELED`로 종결되어야 한다.
* 이때 `cumQty > 0`, `leavesQty = 0`일 수 있어야 한다.
* 취소 요청 중 추가 체결이 발생할 수 있어야 한다.
* 취소 요청 중 전량 체결되면 최종 상태는 `FILLED`여야 한다.
* 취소 거절 시 주문은 수량 기준으로 `LIVE`, `PARTIALLY_FILLED`, `FILLED` 중 하나로 수렴해야 한다.

---

## FR-008. DAY 주문 만료 처리

시스템은 `DAY` 주문의 미체결 잔량 만료를 처리해야 한다.

### 수용 기준

* `LIVE` 주문은 장 마감 시 `EXPIRED`로 전환될 수 있어야 한다.
* `PARTIALLY_FILLED` 주문은 장 마감 시 잔량이 만료되어 `EXPIRED`로 전환될 수 있어야 한다.
* `PENDING_CANCEL` 주문도 장 마감 이벤트에 따라 `EXPIRED`로 수렴할 수 있어야 한다.
* `EXPIRED` 상태에서는 `leavesQty = 0`이어야 한다.
* `cumQty > 0`이고 `EXPIRED`인 경우 부분체결 후 잔량 만료로 해석되어야 한다.

---

## FR-009. 실시간 주문 상태 알림

시스템은 주문 상태 변경을 사용자에게 실시간으로 전달해야 한다.

1차 범위에서는 SSE를 사용한다.

### 수용 기준

* 주문 상태가 변경되면 사용자에게 SSE 이벤트가 전달되어야 한다.
* 부분체결, 완전체결, 취소, 거절, 만료, `UNKNOWN` 진입 이벤트가 전달되어야 한다.
* SSE 연결이 끊겨도 사용자는 주문 조회 API를 통해 최종 상태를 확인할 수 있어야 한다.
* SSE는 상태 저장소가 아니라 알림 채널로만 사용되어야 한다.

---

## FR-010. 외부 브로커 전문 통신

시스템은 외부 브로커 Mock과 TCP 전문 기반으로 통신해야 한다.

### 포함 요소

* TCP length-prefixed frame
* 공통 전문 header
* fixed-length body
* 전문 ID별 parser/serializer
* field padding
* numeric zero padding
* `wireMessageId` 기반 전문 추적 및 중복 이벤트 식별
* `traceId` 기반 처리 흐름 추적

### 수용 기준

* 시스템은 주문 요청 전문을 생성해 Broker Simulator로 전송할 수 있어야 한다.
* 시스템은 취소 요청 전문을 생성해 Broker Simulator로 전송할 수 있어야 한다.
* 시스템은 상태조회 전문을 생성해 Broker Simulator로 전송할 수 있어야 한다.
* 시스템은 브로커 응답 전문을 파싱해 내부 canonical event로 변환할 수 있어야 한다.
* 전문 parsing failure와 business reject는 구분되어야 한다.
* 전문 포맷은 Order Service 도메인 모델에 직접 노출되면 안 된다.

---

## FR-011. Broker Simulator

Broker Simulator는 stateful한 외부 브로커 역할을 수행해야 한다.

### 지원 전문

* 주문 요청
* 주문 접수
* 주문 거절
* 부분체결
* 완전체결
* 취소 요청
* 취소 완료
* 취소 거절
* 주문 만료
* 주문 상태 조회
* 프로토콜 오류 응답

### 수용 기준

* Broker Simulator는 주문별 상태를 유지해야 한다.
* Broker Simulator는 부분체결 후 완전체결 시나리오를 재현할 수 있어야 한다.
* Broker Simulator는 부분체결 후 취소 시나리오를 재현할 수 있어야 한다.
* Broker Simulator는 ACK 유실, 지연, 중복 이벤트, 순서 역전, malformed 전문을 테스트 시나리오로 주입할 수 있어야 한다.
* Broker Simulator는 동일 논리 이벤트를 중복 전송할 경우 동일 `wireMessageId`를 재사용해야 한다.

---

## FR-012. `UNKNOWN` 상태 처리

시스템은 외부 응답이 불확실한 주문을 `UNKNOWN` 상태로 전환할 수 있어야 한다.

### 수용 기준

* 주문 전송 후 ACK timeout이 발생하면 주문은 `UNKNOWN`으로 전환될 수 있어야 한다.
* 취소 요청 후 응답 timeout이 발생하면 주문은 `UNKNOWN`으로 전환될 수 있어야 한다.
* `UNKNOWN` 상태에서는 사용자가 추가 취소 요청을 할 수 없어야 한다.
* `UNKNOWN` 상태는 reconciliation 대상이어야 한다.
* `UNKNOWN` 진입 사유는 추적 가능해야 한다.

---

## FR-013. Reconciliation

시스템은 `UNKNOWN` 상태 주문을 브로커 상태조회 기반으로 복구해야 한다.

### 수용 기준

* `UNKNOWN` 주문에 대해 reconciliation job이 생성되어야 한다.
* Recovery Service는 브로커 상태조회 명령을 발행해야 한다.
* 상태조회 결과에 따라 주문은 `LIVE`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `REJECTED`, `EXPIRED` 중 하나로 수렴해야 한다.
* reconciliation 성공/실패 이력은 추적 가능해야 한다.
* reconciliation 결과가 사용자 조회 상태에 반영되어야 한다.

---

## FR-014. 운영 추적 정보 제공

시스템은 장애 분석에 필요한 최소 운영 이력을 남겨야 한다.

### 필수 이력

* 주문 상태 변경 이력
* 브로커 전문 송수신 journal
* command attempt 이력
* reconciliation job 이력
* malformed 전문 처리 이력
* 메시지 처리 실패 이력

### 수용 기준

* 특정 주문의 상태 변경 원인을 추적할 수 있어야 한다.
* 특정 `orderId`, `wireMessageId`, `traceId` 기준으로 흐름을 추적할 수 있어야 한다.
* `UNKNOWN` 진입 원인과 reconciliation 결과를 확인할 수 있어야 한다.

---

# 3.5 비기능 요구사항

## NFR-001. 정합성

시스템은 외부 브로커 이벤트가 중복, 지연, 순서 역전되어도 주문 상태를 일관되게 유지해야 한다.

### 기준

* 주문 상태의 source of truth는 Order Service DB다.
* 주문 상태 변경은 Order Service만 수행한다.
* 중복 체결 이벤트는 누적 체결 수량에 중복 반영되면 안 된다.
* 종결 상태의 주문은 잘못된 후속 이벤트로 상태가 오염되면 안 된다.
* `UNKNOWN` 상태는 reconciliation을 통해 최종 상태로 수렴 가능해야 한다.

---

## NFR-002. 메시지 발행 신뢰성

시스템은 DB 상태 변경과 비동기 메시지 발행 사이의 장애로 인해 복구 불가능한 메시지 유실이 발생하지 않도록 설계되어야 한다.

### 기준

* 주문 생성 후 브로커 전송 요청이 영구 유실되면 안 된다.
* 주문 취소 후 브로커 취소 요청이 영구 유실되면 안 된다.
* 브로커 전문 수신 이력이 저장되었는데 canonical event 발행이 영구 유실되면 안 된다.
* reconciliation job이 생성되었는데 상태조회 command가 영구 유실되면 안 된다.
* 메시지 발행 실패는 재시도 가능해야 한다.

### 구현 방향

* 서비스별 Outbox 패턴 적용을 후보로 둔다.
* 최종 채택 여부와 세부 방식은 ADR에서 결정한다.

---

## NFR-003. 메시지 소비 멱등성

시스템은 비동기 메시지가 중복 소비되더라도 주문 상태, 브로커 전송, reconciliation job이 중복 처리로 오염되지 않도록 설계되어야 한다.

### 기준

* 동일 broker event가 중복 소비되어도 주문 상태가 중복 갱신되면 안 된다.
* 동일 command가 중복 소비되어도 브로커 주문 전문이 중복 전송되면 안 된다.
* 동일 lifecycle event가 중복 소비되어도 reconciliation job이 중복 생성되면 안 된다.
* consumer 처리 실패 이후 재처리해도 상태가 오염되면 안 된다.

### 구현 방향

* 서비스별 Inbox 또는 processed message 기록을 후보로 둔다.
* 이벤트별 semantic dedup key 사용을 후보로 둔다.
* 최종 채택 여부와 세부 방식은 ADR에서 결정한다.

---

## NFR-004. 장애 격리

외부 브로커 장애는 주문 시스템 전체 장애로 전파되지 않아야 한다.

### 기준

* 브로커 응답 timeout이 발생해도 주문 데이터가 유실되면 안 된다.
* 브로커 응답이 애매하면 `UNKNOWN`으로 격리되어야 한다.
* `UNKNOWN` 주문은 reconciliation 대상이 되어야 한다.
* 브로커 통신 실패가 Order Service API 전체 장애로 확산되면 안 된다.

---

## NFR-005. 관측성

시스템은 주문 상태 변경과 외부 브로커 연계 흐름을 추적할 수 있어야 한다.

### 기준

* 주문 상태 변경은 이벤트 이력으로 남아야 한다.
* 브로커 전문 송수신 원문과 파싱 결과를 추적할 수 있어야 한다.
* `orderId`, `wireMessageId`, `traceId` 기준으로 추적 가능해야 한다.
* reconciliation job의 생성, 실행, 성공, 실패를 추적할 수 있어야 한다.
* 주요 메트릭을 수집해야 한다.

### 주요 메트릭 후보

* 주문 생성 수
* 주문 상태별 개수
* 브로커 ACK latency
* 체결 이벤트 처리 latency
* timeout 발생 수
* `UNKNOWN` 진입 수
* reconciliation 성공/실패 수
* malformed 전문 수
* Kafka publish 실패 수
* Kafka consume 실패 수

---

## NFR-006. 성능 및 지연 관측 가능성

시스템은 주문 생성, 브로커 ACK 반영, 체결 이벤트 반영, reconciliation 수행 과정의 latency를 측정할 수 있어야 한다.

이 항목의 수치는 프로덕션 SLO가 아니라, 1차 구현 완료 후 성능 회귀 여부를 판단하기 위한 **초기 벤치마크 목표 후보**다.

### 전제

* 로컬 또는 단일 개발 환경 기준
* 단일 브로커
* 브로커 지연 시나리오 미주입
* 정상 주문 흐름 기준
* 실제 운영 SLO가 아니라 프로젝트 내부 기준

### 초기 벤치마크 목표 후보

| 구간                                     |    초기 목표 |
| -------------------------------------- | -------: |
| 주문 생성 API p95                          | 200ms 이하 |
| 주문 조회 API p95                          | 100ms 이하 |
| 주문 생성 후 `PENDING_ACK` 저장까지 p95         | 200ms 이하 |
| 브로커 ACK 수신 후 Order DB 상태 반영까지 p95      | 500ms 이하 |
| Order DB 상태 변경 후 SSE 전달까지 p95          |    1초 이하 |
| `UNKNOWN` 진입 후 reconciliation 완료까지 p95 |   60초 이하 |

---

## NFR-007. 확장성

시스템은 향후 멀티 브로커 라우팅과 fallback 확장을 고려해야 한다.

### 기준

* Order Service 도메인 모델은 특정 브로커 전문 포맷에 의존하면 안 된다.
* Broker Gateway는 브로커 어댑터를 분리할 수 있어야 한다.
* Broker Simulator는 향후 Broker A/B로 확장 가능해야 한다.
* 브로커 선택 정책은 주문 도메인 핵심 로직과 분리 가능해야 한다.

---

## NFR-008. 유지보수성

시스템은 도메인 로직, 메시징 로직, 전문 통신 로직이 명확히 분리되어야 한다.

### 기준

* 주문 상태머신은 독립적으로 테스트 가능해야 한다.
* TCP 전문 parser/serializer는 Order Service에 노출되면 안 된다.
* canonical event는 브로커 전문 포맷과 내부 주문 도메인 사이의 경계 역할을 해야 한다.
* 새로운 브로커 통신 방식이 추가되어도 주문 상태 모델을 재작성하지 않아야 한다.

---

## NFR-009. 테스트 가능성

시스템은 정상 흐름뿐 아니라 장애, 경합, 중복, 순서 역전 시나리오를 재현 가능해야 한다.

### 기준

* Broker Simulator는 테스트 시나리오를 주입할 수 있어야 한다.
* ACK 유실, 중복 체결, ACK보다 선행하는 체결 이벤트를 재현할 수 있어야 한다.
* 부분체결 후 취소 경합을 재현할 수 있어야 한다.
* malformed 전문을 주입할 수 있어야 한다.
* 상태머신, 전문 parser, reconciliation 흐름은 자동화 테스트로 검증되어야 한다.

---

## NFR-010. 최소 보안

1차 범위에서 실제 인증/인가 시스템은 구현하지 않는다.
다만 사용자 간 주문 접근 경계와 운영 API 분리는 최소 수준으로 보장한다.

### 기준

* 사용자 요청은 account/user 식별자 기준으로 주문 접근 범위를 제한해야 한다.
* 다른 사용자의 주문을 조회하거나 취소할 수 없어야 한다.
* 운영/테스트용 API는 일반 사용자 API와 분리되어야 한다.
* Broker Simulator admin API는 로컬/테스트 환경 전용으로 둔다.
* 민감 정보는 로그에 남기지 않는다.

---

# 3.6 요구사항 우선순위

## Must Have

* 주문 생성
* 주문 생성 멱등성
* 주문 조회
* 주문 취소
* 주문 상태 전이
* 부분체결 처리
* 부분체결 후 취소 처리
* DAY 주문 만료 처리
* SSE 상태 알림
* TCP 전문 기반 Broker Simulator
* Broker 전문 → canonical event 변환
* `UNKNOWN` 상태 처리
* Reconciliation
* 중복 이벤트 방어
* 기본 운영 이력

## Should Have

* malformed 전문 처리
* ACK보다 먼저 도착하는 체결 이벤트 처리
* 부분체결 후 취소 경합 처리
* 기본 메트릭 수집
* Broker Simulator scenario admin API
* 주문 이벤트 타임라인 조회 API

## Could Have

* 간단한 Grafana dashboard
* DLQ 조회
* 수동 reconciliation trigger
* 브로커별 지연 메트릭
* 테스트 리포트 자동 생성

## Won’t Have in Phase 1

* 실제 브로커 연동
* 실제 FIX
* 멀티 브로커 라우팅
* fallback
* 운영 콘솔 UI
* buying power
* position
* 잔고 관리
* 계좌 관리
* 정산
* 환전
* 세금
* 실시간 시세
* 복잡한 시장 세션 캘린더
