# 10-A. Broker TCP 전문 Byte-Level Layout

## 10-A.1 목적

이 문서는 Broker Gateway Service와 Broker Simulator 사이에서 사용하는 TCP 전문의 byte-level layout을 정의한다.

10장 본문은 API, Kafka message, canonical broker event의 논리 계약을 다룬다. 이 appendix는 TCP 전문에 한정해 다음을 확정한다.

1. TCP frame length header 표현 방식
2. common header의 byte offset과 길이
3. msgId별 fixed-length body layout
4. 문자열 padding, 숫자 zero padding, 가격 표현 방식
5. malformed 전문 판정 기준
6. Gateway가 전문을 canonical broker event로 변환할 때 사용하는 기본 매핑 규칙

이 layout은 Phase 1의 Broker Simulator 전용 프로토콜이다. 실제 증권사, FIX, ISO 8583, FAST, SBE 같은 외부 표준을 구현하려는 목적이 아니다.

---

## 10-A.2 채택 Layout

Phase 1에서는 **전체 ASCII 고정폭 전문**을 사용한다.

```text
[ASCII decimal length header][ASCII fixed common header][ASCII fixed body]
```

채택 기준은 다음이다.

| 항목 | 결정 |
| --- | --- |
| length header | 8 byte ASCII decimal |
| common header | ASCII fixed-length fields |
| body | msgId별 ASCII fixed-length fields |
| 문자열 | US-ASCII, right space padding |
| 숫자 | US-ASCII digit, left zero padding |
| 가격 | scale 4 정수 문자열 |

이 방식은 raw journal을 사람이 직접 읽기 쉽고, malformed fixture를 텍스트로 만들기 쉽다. Phase 1의 목적은 wire efficiency가 아니라 외부 브로커 연계의 불확실성, 전문 parsing, malformed 격리, 상태 수렴을 검증하는 것이므로 가독성과 테스트 용이성을 우선한다.

---

# 10-A.3 공통 Encoding 규칙

## 10-A.3.1 Character Set

모든 전문은 **US-ASCII**로 인코딩한다.

| 필드 유형 | 허용 문자 |
| --- | --- |
| alphabetic code | `A-Z`, `_` |
| numeric | `0-9` |
| UUID string | `0-9`, `a-f`, `-` |
| free text reason | printable ASCII |
| padding | space `0x20` |

전문에는 delimiter, newline, null terminator를 넣지 않는다.

---

## 10-A.3.2 Field Type

| 타입 | 의미 | Padding |
| --- | --- | --- |
| `A(n)` | ASCII 문자열 | right space padding |
| `N(n)` | 음수 없는 정수 | left zero padding |
| `P(n)` | scale 4 decimal price를 정수화한 값 | left zero padding |
| `TS17` | UTC timestamp, `yyyyMMddHHmmssSSS` | padding 없음 |
| `UUID36` | UUID 문자열 | padding 없음 |

예:

| 값 | 타입 | Wire 표현 |
| --- | --- | --- |
| `AAPL` | `A(16)` | `AAPL            ` |
| `100` | `N(18)` | `000000000000000100` |
| `189.50` | `P(18)` | `000000000001895000` |
| `2026-05-13T01:15:35.500Z` | `TS17` | `20260513011535500` |

`P(18)`은 decimal scale 4를 사용한다.

```text
wire price = decimal price * 10000
189.50 -> 1895000 -> 000000000001895000
```

Phase 1에서는 음수 수량, 음수 가격, 소수점 4자리 초과 가격을 허용하지 않는다.

---

## 10-A.3.3 Blank Field

선택적 문자열 값이 없으면 전체를 space로 채운다.

선택적 숫자 값이 없으면 전체를 zero로 채운다.

| 의미 | 타입 | Wire 표현 |
| --- | --- | --- |
| brokerOrderId 없음 | `A(64)` | 64 spaces |
| rejectCode 없음 | `A(16)` | 16 spaces |
| cumQty 없음 | `N(18)` | `000000000000000000` |

---

# 10-A.4 Frame 구조

## 10-A.4.1 전체 구조

```text
[length header: N(8)][common header: 192 bytes][body: bodyLength bytes]
```

| 영역 | 길이 | 설명 |
| --- | ---: | --- |
| length header | 8 | common header + body 길이 |
| common header | 192 | 모든 전문 공통 metadata |
| body | msgId별 고정 | `bodyLength`와 같아야 함 |

`length header`는 자기 자신 8 byte를 제외한 길이다.

```text
frameLength = commonHeaderLength + bodyLength
```

예:

```text
ORDR bodyLength = 91
frameLength = 192 + 91 = 283
length header = 00000283
```

TCP stream decoder는 먼저 8 byte를 읽어 `frameLength`를 구하고, 이후 정확히 `frameLength` byte를 더 읽어 하나의 전문으로 처리한다.

---

## 10-A.4.2 Common Header Layout

Offset은 length header 직후를 기준으로 한다.

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 4 | `A(4)` | `msgId` | 전문 ID |
| 4 | 2 | `N(2)` | `protocolVersion` | Phase 1은 `01` |
| 6 | 5 | `N(5)` | `bodyLength` | body byte 길이 |
| 11 | 64 | `A(64)` | `wireMessageId` | 전문 단위 ID |
| 75 | 36 | `UUID36` | `orderId` | 내부 주문 ID |
| 111 | 64 | `A(64)` | `traceId` | end-to-end 추적 ID |
| 175 | 17 | `TS17` | `sentAtUtc` | 전문 생성 시각 |

Common header 총 길이는 **192 bytes**다.

---

## 10-A.4.3 Common Header 규칙

| 필드 | 규칙 |
| --- | --- |
| `msgId` | 10-A.5의 전문 ID만 허용 |
| `protocolVersion` | Phase 1에서는 `01`만 허용 |
| `bodyLength` | msgId별 body length와 정확히 일치해야 함 |
| `wireMessageId` | 송신자가 생성. 요청/응답 관계에서는 응답이 요청 값을 echo |
| `orderId` | Phase 1에서는 모든 정상 전문에 필수 |
| `traceId` | 없으면 Gateway 또는 Simulator가 생성한 값을 사용 |
| `sentAtUtc` | UTC 기준 `yyyyMMddHHmmssSSS` |

응답 전문의 `wireMessageId` 규칙:

| 상황 | 규칙 |
| --- | --- |
| `ORDR`에 대한 `ACKN` / `RJCT` | `ORDR.wireMessageId` echo |
| `CXLQ`에 대한 `CXLA` / `CXLR` | `CXLQ.wireMessageId` echo |
| `OSTQ`에 대한 `OSTS` | `OSTQ.wireMessageId` echo |
| 비동기 `FILL` / `EXPR` | Broker Simulator가 새 `wireMessageId` 생성 |
| 중복 비동기 이벤트 재전송 | 동일 논리 이벤트면 동일 `wireMessageId` 재사용 |

---

# 10-A.5 전문 ID 목록

| msgId | 방향 | 의미 | Body Length |
| --- | --- | --- | ---: |
| `ORDR` | Gateway -> Broker | 주문 요청 | 91 |
| `ACKN` | Broker -> Gateway | 주문 접수 | 81 |
| `RJCT` | Broker -> Gateway | 주문 거절 | 96 |
| `FILL` | Broker -> Gateway | 체결 | 200 |
| `CXLQ` | Gateway -> Broker | 취소 요청 | 64 |
| `CXLA` | Broker -> Gateway | 취소 완료 | 81 |
| `CXLR` | Broker -> Gateway | 취소 거절 | 160 |
| `EXPR` | Broker -> Gateway | DAY 주문 만료 | 117 |
| `OSTQ` | Gateway -> Broker | 주문 상태조회 | 168 |
| `OSTS` | Broker -> Gateway | 주문 상태조회 응답 | 217 |

---

# 10-A.6 Body Layout

## 10-A.6.1 `ORDR` 주문 요청

Direction: Gateway -> Broker

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 32 | `A(32)` | `accountId` | Phase 1 계좌 식별자 |
| 32 | 2 | `A(2)` | `market` | Phase 1은 `US` |
| 34 | 16 | `A(16)` | `symbol` | 종목 코드 |
| 50 | 1 | `A(1)` | `side` | `B` = BUY, `S` = SELL |
| 51 | 1 | `A(1)` | `orderType` | `L` = LIMIT |
| 52 | 3 | `A(3)` | `tif` | `DAY` |
| 55 | 18 | `N(18)` | `orderQty` | 주문 수량 |
| 73 | 18 | `P(18)` | `limitPrice` | scale 4 지정가 |

Body length: **91**

---

## 10-A.6.2 `ACKN` 주문 접수

Direction: Broker -> Gateway

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 64 | `A(64)` | `brokerOrderId` | 브로커 주문 ID |
| 64 | 17 | `TS17` | `acceptedAtUtc` | 브로커 접수 시각 |

Body length: **81**

Canonical event:

```text
BrokerOrderAcknowledged
```

---

## 10-A.6.3 `RJCT` 주문 거절

Direction: Broker -> Gateway

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 16 | `A(16)` | `rejectCode` | 거절 코드 |
| 16 | 80 | `A(80)` | `rejectReason` | 거절 사유 |

Body length: **96**

Canonical event:

```text
BrokerOrderRejected
```

---

## 10-A.6.4 `FILL` 체결

Direction: Broker -> Gateway

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 64 | `A(64)` | `brokerOrderId` | 브로커 주문 ID |
| 64 | 64 | `A(64)` | `executionId` | 브로커 체결 ID |
| 128 | 1 | `A(1)` | `fillStatus` | `P` = partial, `F` = full |
| 129 | 18 | `N(18)` | `lastFillQty` | 이번 체결 수량 |
| 147 | 18 | `N(18)` | `cumQty` | 누적 체결 수량 |
| 165 | 18 | `N(18)` | `leavesQty` | 잔여 수량 |
| 183 | 17 | `TS17` | `filledAtUtc` | 체결 시각 |

Body length: **200**

Canonical event:

| 조건 | Event |
| --- | --- |
| `fillStatus = P` and `leavesQty > 0` | `BrokerOrderPartiallyFilled` |
| `fillStatus = F` or `leavesQty = 0` | `BrokerOrderFilled` |

`fillStatus = P`인데 `leavesQty = 0`이면 Gateway는 `BrokerOrderFilled`로 정규화한다. 이 경우 `payloadHash`와 journal에 원문을 남겨 분석 가능하게 한다.

---

## 10-A.6.5 `CXLQ` 취소 요청

Direction: Gateway -> Broker

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 64 | `A(64)` | `brokerOrderId` | 있으면 브로커 주문 ID, 없으면 blank |

Body length: **64**

`brokerOrderId`가 blank이면 Broker Simulator는 header의 `orderId`로 취소 대상 주문을 찾는다. Phase 1에서는 Simulator가 `orderId`를 보유하므로 이 fallback을 허용한다.

이 규칙은 사용자가 `PENDING_ACK` 주문에 대해 취소를 요청했지만 Gateway가 아직 `brokerOrderId` binding을 확보하지 못한 경우를 처리하기 위한 것이다.

---

## 10-A.6.6 `CXLA` 취소 완료

Direction: Broker -> Gateway

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 64 | `A(64)` | `brokerOrderId` | 브로커 주문 ID |
| 64 | 17 | `TS17` | `canceledAtUtc` | 취소 완료 시각 |

Body length: **81**

Canonical event:

```text
BrokerCancelAcknowledged
```

---

## 10-A.6.7 `CXLR` 취소 거절

Direction: Broker -> Gateway

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 64 | `A(64)` | `brokerOrderId` | 브로커 주문 ID |
| 64 | 16 | `A(16)` | `rejectCode` | 취소 거절 코드 |
| 80 | 80 | `A(80)` | `rejectReason` | 취소 거절 사유 |

Body length: **160**

Canonical event:

```text
BrokerCancelRejected
```

---

## 10-A.6.8 `EXPR` DAY 주문 만료

Direction: Broker -> Gateway

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 64 | `A(64)` | `brokerOrderId` | 브로커 주문 ID |
| 64 | 18 | `N(18)` | `cumQty` | 누적 체결 수량 |
| 82 | 18 | `N(18)` | `leavesQty` | 만료 후 잔여 수량. 정상은 `0` |
| 100 | 17 | `TS17` | `expiredAtUtc` | 만료 시각 |

Body length: **117**

Canonical event:

```text
BrokerOrderExpired
```

`EXPR.leavesQty`가 `0`이 아니면 Gateway는 malformed가 아니라 business semantic anomaly로 기록하고, canonical event에는 원문 값을 포함한다. 최종 수량 불변식 판단은 Order Service가 수행한다.

---

## 10-A.6.9 `OSTQ` 주문 상태조회

Direction: Gateway -> Broker

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 36 | `UUID36` | `jobId` | reconciliation job ID |
| 36 | 36 | `UUID36` | `attemptId` | reconciliation attempt ID |
| 72 | 64 | `A(64)` | `brokerOrderId` | 있으면 브로커 주문 ID, 없으면 blank |
| 136 | 32 | `A(32)` | `triggerType` | reconciliation trigger |

Body length: **168**

`brokerOrderId`가 blank이면 Broker Simulator는 header의 `orderId`로 상태를 조회한다. Phase 1에서는 Simulator가 `orderId`를 보유하므로 이 fallback을 허용한다.

---

## 10-A.6.10 `OSTS` 주문 상태조회 응답

Direction: Broker -> Gateway

| Offset | Length | Type | Field | 설명 |
| ---: | ---: | --- | --- | --- |
| 0 | 36 | `UUID36` | `jobId` | reconciliation job ID |
| 36 | 36 | `UUID36` | `attemptId` | reconciliation attempt ID |
| 72 | 64 | `A(64)` | `brokerOrderId` | 브로커 주문 ID. `NOT_FOUND`면 blank 가능 |
| 136 | 12 | `A(12)` | `snapshotStatus` | 상태조회 결과 |
| 148 | 18 | `N(18)` | `cumQty` | 누적 체결 수량 |
| 166 | 18 | `N(18)` | `leavesQty` | 잔여 수량 |
| 184 | 16 | `A(16)` | `rejectCode` | 거절 상태가 아니면 blank |
| 200 | 17 | `TS17` | `snapshotAtUtc` | snapshot 생성 시각 |

Body length: **217**

`snapshotStatus` 값:

| Wire 값 | Canonical 값 |
| --- | --- |
| `ACCEPTED` | `ACCEPTED` |
| `PARTIAL` | `PARTIAL` |
| `FILLED` | `FILLED` |
| `CANCELED` | `CANCELED` |
| `REJECTED` | `REJECTED` |
| `EXPIRED` | `EXPIRED` |
| `NOT_FOUND` | `NOT_FOUND` |

Canonical event:

```text
BrokerOrderStatusSnapshot
```

---

# 10-A.7 코드 값

## 10-A.7.1 `side`

| Wire | 의미 |
| --- | --- |
| `B` | BUY |
| `S` | SELL |

## 10-A.7.2 `orderType`

| Wire | 의미 |
| --- | --- |
| `L` | LIMIT |

## 10-A.7.3 `tif`

| Wire | 의미 |
| --- | --- |
| `DAY` | Day order |

## 10-A.7.4 대표 `rejectCode`

| Code | 의미 |
| --- | --- |
| `INVALID_PRICE` | 가격 오류 |
| `INVALID_QTY` | 수량 오류 |
| `MARKET_CLOSED` | 시장 종료 |
| `UNKNOWN_ORDER` | 브로커 주문을 찾을 수 없음 |
| `TOO_LATE_CANCEL` | 취소하기에 너무 늦음 |

`rejectCode`는 `A(16)`이므로 16 byte를 넘을 수 없다. 긴 도메인 코드는 Gateway에서 canonical event의 `rejectMessage`에 보존하거나 내부 표준 코드로 매핑한다.

---

# 10-A.8 전문 예시

## 10-A.8.1 `ORDR` 예시

논리 값:

```text
msgId = ORDR
protocolVersion = 01
bodyLength = 00091
wireMessageId = W-GW-20260513-0001
orderId = 018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa
traceId = trace-order-20260513-0001
sentAtUtc = 20260513011530123
accountId = ACC-001
market = US
symbol = AAPL
side = B
orderType = L
tif = DAY
orderQty = 100
limitPrice = 189.50
```

Frame length:

```text
common header 192 + ORDR body 91 = 283
length header = 00000283
```

구분 표시용 표현:

```text
00000283
ORDR0100091W-GW-20260513-0001                                          018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aatrace-order-20260513-0001                                      20260513011530123
ACC-001                         USAAPL            BLDAY000000000000000100000000000001895000
```

실제 wire에는 줄바꿈이 없다.

---

## 10-A.8.2 `FILL` 예시

```text
msgId = FILL
bodyLength = 00200
brokerOrderId = BRK-ORDER-0001
executionId = EXEC-0001
fillStatus = P
lastFillQty = 40
cumQty = 40
leavesQty = 60
filledAtUtc = 20260513011800100
```

Gateway 정규화:

```text
FILL(fillStatus=P, leavesQty=60)
  -> BrokerOrderPartiallyFilled(lastFillQty=40, cumQty=40, leavesQty=60)
```

---

# 10-A.9 Malformed 판정 기준

Gateway는 TCP 전문 수신 시 다음 순서로 검증한다.

1. length header 8 byte를 읽을 수 있는지 확인한다.
2. length header가 `N(8)`인지 확인한다.
3. `frameLength`가 `192` 이상인지 확인한다.
4. `frameLength`만큼 payload를 읽는다.
5. common header 192 byte를 parse한다.
6. `protocolVersion`, `msgId`, `bodyLength`를 검증한다.
7. msgId별 body parser로 body를 parse한다.
8. enum, timestamp, numeric, UUID, 수량/가격 field를 검증한다.

## 10-A.9.1 Frame malformed

| 조건 | 처리 |
| --- | --- |
| length header가 8 byte 미만 | connection/frame error로 journal 기록 |
| length header가 숫자가 아님 | malformed frame |
| `frameLength < 192` | malformed frame |
| 실제 수신 byte 수가 `frameLength`보다 짧음 | frame incomplete. timeout 후 malformed 또는 connection error |
| `frameLength != 192 + bodyLength` | malformed frame |

Frame malformed는 보통 `orderId`를 신뢰할 수 없으므로 주문 상태를 직접 변경하지 않는다.

---

## 10-A.9.2 Header malformed

| 조건 | 처리 |
| --- | --- |
| 알 수 없는 `msgId` | journal/metric 기록, 상태 변경 없음 |
| 지원하지 않는 `protocolVersion` | journal/metric 기록, 상태 변경 없음 |
| `bodyLength`가 msgId별 고정 길이와 다름 | pending command와 매칭 가능하면 outcome unknown, 아니면 상태 변경 없음 |
| `orderId`가 UUID 형식이 아님 | 상태 변경 없음 |
| `sentAtUtc` parse 실패 | malformed header |

---

## 10-A.9.3 Body malformed

| 조건 | 처리 |
| --- | --- |
| numeric field에 숫자 외 문자가 있음 | body malformed |
| price field scale 변환 실패 | body malformed |
| enum 값이 허용 목록 밖임 | body malformed |
| required string field가 blank임 | body malformed |
| timestamp parse 실패 | body malformed |

처리 경로:

| 상황 | Gateway 처리 |
| --- | --- |
| pending command의 응답이고 `wireMessageId`가 매칭됨 | `BrokerCommandOutcomeUnknown` 발행 |
| 비동기 이벤트이며 `orderId`와 body를 신뢰할 수 있음 | 가능한 경우 canonical event 발행, anomaly 포함 |
| 비동기 이벤트이며 주문 귀속 불가 | journal/metric만 기록, 상태 변경 없음 |

---

## 10-A.9.4 Business semantic anomaly

다음은 byte-level malformed가 아니라 도메인 의미상 이상한 전문이다.

| 조건 | 처리 |
| --- | --- |
| `FILL.cumQty < lastFillQty` | canonical event 발행 가능, Order Service가 수량 불변식으로 판단 |
| `FILL.leavesQty = 0`인데 `fillStatus = P` | `BrokerOrderFilled`로 정규화하고 anomaly 기록 |
| `EXPR.leavesQty != 0` | canonical event 발행 가능, Order Service가 판단 |
| `OSTS.snapshotStatus = NOT_FOUND`인데 수량이 0이 아님 | snapshot 발행 가능, Order Service가 자동 종결 금지 정책 적용 |

원칙:

> Gateway는 byte parsing과 protocol validity를 판단한다. 주문 수량 불변식과 상태 전이 가능성은 Order Service가 판단한다.

---

# 10-A.10 Dedup Key와 Payload Hash

Gateway는 정상 parse된 브로커 전문을 canonical broker event로 변환할 때 다음 값을 생성한다.

## 10-A.10.1 `brokerEventDedupKey`

기본 규칙:

```text
brokerCode + ":" + msgId + ":" + wireMessageId
```

예:

```text
SIM:ACKN:W-GW-20260513-0001
SIM:FILL:W-BRK-20260513-0101
```

동일 논리 브로커 이벤트가 재전송될 때 Broker Simulator는 같은 `wireMessageId`를 재사용한다. 따라서 Order Service는 `brokerEventDedupKey` 기준으로 중복 반영을 방지할 수 있다.

## 10-A.10.2 `payloadHash`

`payloadHash`는 common header와 body의 원문 byte를 대상으로 SHA-256을 계산한다.

```text
payloadHash = sha256(commonHeaderBytes + bodyBytes)
```

length header는 hash 대상에서 제외한다.

---

# 10-A.11 Parser / Serializer 구현 지침

Phase 1에서는 `libs/broker-protocol`에 다음 책임을 둔다.

| 모듈 책임 | 설명 |
| --- | --- |
| frame decoder/encoder | length header와 frame boundary 처리 |
| common header codec | 192 byte common header parse/serialize |
| body codec registry | msgId별 body parser/serializer dispatch |
| field codec | `A(n)`, `N(n)`, `P(n)`, `TS17`, `UUID36` 처리 |
| malformed error model | frame/header/body/business anomaly 분류 |

Gateway와 Simulator는 같은 codec을 사용하되, 다음 책임은 분리한다.

| 위치 | 책임 |
| --- | --- |
| Broker Gateway | TCP client, command attempt, journal, canonical event 변환 |
| Broker Simulator | TCP server, broker-side state, scenario injection |
| `broker-protocol` library | 순수 byte codec과 validation |

---

# 10-A.12 확정 사항 요약

| 항목 | 결정 |
| --- | --- |
| layout | 전체 ASCII 고정폭 전문 |
| charset | US-ASCII |
| length header | `N(8)`, common header + body 길이 |
| common header length | 192 bytes |
| body | msgId별 fixed-length |
| timestamp | UTC `yyyyMMddHHmmssSSS` |
| price | scale 4 정수, `P(18)` |
| string padding | right space padding |
| numeric padding | left zero padding |
| order reference | Phase 1에서는 common header에 `orderId` 포함 |
| response correlation | 요청/응답은 `wireMessageId` echo |
| async event dedup | 동일 논리 이벤트는 동일 `wireMessageId` 재사용 |
| malformed 격리 | Gateway에서 frame/header/body malformed 분류 |
| 도메인 상태 판단 | Order Service 상태머신이 수행 |
