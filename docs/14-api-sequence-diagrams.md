# 14. API Sequence Diagrams

## 14.1 목적

이 문서는 프로젝트 내 주요 사용자-facing API와 내부 처리 흐름을 sequence diagram으로 정리한다.

주요 목적은 다음이다.

1. API 요청이 어떤 application/domain/persistence/messaging 경계를 통과하는지 한눈에 확인한다.
2. 동기 API 응답 범위와 비동기 메시징 후속 처리를 분리해 설명한다.
3. 멱등성, outbox, traceId, 상태 전이 같은 핵심 invariant가 흐름에서 어디에 적용되는지 기록한다.
4. 이후 API별 설계 검토, 테스트 보강, 운영 추적 문서의 출발점으로 사용한다.

이 문서는 구현 코드의 클래스명을 일부 포함한다.
계약 수준의 API/Event/Protocol 정의는 `docs/10-api-event-protocol-spec.md`를 우선한다.

---

## 14.2 작성 규칙

새 API diagram을 추가할 때는 다음 순서를 따른다.

1. 사용자-facing request/response 범위를 먼저 표시한다.
2. 같은 DB transaction 안에서 수행되는 작업은 `rect` 또는 `Note`로 묶는다.
3. outbox/Kafka/consumer 후속 처리는 API 응답 이후의 비동기 흐름으로 분리한다.
4. 오류 흐름은 멱등성 충돌, 유효성 오류, 상태 전이 거절처럼 설계 invariant에 중요한 경우만 포함한다.
5. 아직 구현되지 않은 후속 milestone 범위는 명시적으로 표시한다.

---

## 14.3 `POST /api/orders` 신규 주문 생성

### 범위

현재 M2 구현 기준으로 `POST /api/orders`는 외부 브로커 ACK를 동기적으로 기다리지 않는다.
Order Service는 주문과 `PLACE` instruction, 주문 이벤트, `SubmitOrderCommand` outbox를 같은 DB transaction 안에 저장한 뒤 `PENDING_ACK` 응답을 반환한다.
outbox publisher가 이후 Kafka topic `trading.broker.command.v1`로 broker command를 비동기 발행한다.

### Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant OC as OrderController
    participant AS as OrderApplicationService
    participant HS as HashingService
    participant IR as OrderInstructionRepository
    participant OR as TradeOrderRepository
    participant ER as OrderEventRepository
    participant OB as OutboxMessageRepository
    participant DB as order_db
    participant OP as OutboxPublisher/Scheduler
    participant K as Kafka<br/>trading.broker.command.v1
    participant BG as Broker Gateway<br/>(M3+ design flow)
    participant B as Broker Simulator<br/>(M3+ design flow)

    C->>OC: POST /api/orders<br/>X-Trace-Id?, CreateOrderRequest
    OC->>OC: Validate request body, required values, length limits
    OC->>OC: Build PlaceOrderCommand<br/>including value object validation

    OC->>AS: createOrder(command)

    rect rgb(245,245,245)
        Note over AS,DB: @Transactional

        AS->>HS: canonicalJson(idempotency payload)
        HS-->>AS: payloadJson
        AS->>HS: sha256(payloadJson)
        HS-->>AS: payloadHash

        AS->>IR: findByIdempotencyKey(accountId, PLACE, clientOrderId)
        IR->>DB: SELECT order_instruction

        alt Existing PLACE instruction + same payloadHash
            IR-->>AS: existingInstruction
            AS->>OR: findById(existingInstruction.orderId)
            OR->>DB: SELECT trade_order
            OR-->>AS: existingOrder
            AS-->>OC: PlaceOrderResult(existingOrder, created=false)
            OC-->>C: 200 OK + existing order response
        else Existing PLACE instruction + different payloadHash
            AS-->>OC: IdempotencyConflictException
            OC-->>C: 409 Conflict
        else No existing instruction
            AS->>AS: Check marketState == OPEN

            alt Market CLOSED
                AS-->>OC: OrderRequestRejectedException(MARKET_CLOSED)
                OC-->>C: 409 Conflict
            else Market OPEN
                AS->>AS: Order.createPendingAck()<br/>status=PENDING_ACK, cumQty=0, leavesQty=orderQty
                AS->>AS: Create PLACE OrderInstruction<br/>status=REQUESTED

                AS->>IR: insert(instruction, payloadJson)
                IR->>DB: INSERT order_instruction<br/>UK(accountId, instructionType, clientInstructionId)

                alt Concurrent retry duplicate key
                    IR-->>AS: DuplicateKeyException
                    AS->>AS: Resolve in REQUIRES_NEW transaction
                    AS->>IR: findByIdempotencyKey(accountId, PLACE, clientOrderId)
                    IR->>DB: SELECT order_instruction
                    IR-->>AS: existingInstruction
                    AS->>OR: findById(existingInstruction.orderId)
                    OR->>DB: SELECT trade_order
                    OR-->>AS: existingOrder
                    AS-->>OC: PlaceOrderResult(existingOrder, created=false)
                    OC-->>C: 200 OK + existing order response
                else New instruction inserted
                    AS->>OR: insert(order)
                    OR->>DB: INSERT trade_order

                    AS->>ER: insert(OrderCreated)
                    ER->>DB: INSERT order_event

                    AS->>ER: insert(PlaceInstructionCreated)
                    ER->>DB: INSERT order_event

                    AS->>OB: appendBrokerCommand(SubmitOrderCommand)
                    OB->>DB: INSERT outbox_message<br/>status=READY, topic=trading.broker.command.v1,<br/>messageKey=orderId, traceId header

                    AS-->>OC: PlaceOrderResult(order, created=true)
                    OC-->>C: 201 Created<br/>Location: /api/orders/{orderId}<br/>status=PENDING_ACK
                end
            end
        end
    end

    opt Outbox publisher enabled
        OP->>OB: claimPublishable()
        OB->>DB: SELECT READY/FAILED publishable<br/>mark PUBLISHING
        OB-->>OP: OutboxMessageRecord
        OP->>OP: Build MessageEnvelope<br/>messageId, messageType, messageKey, occurredAt, traceId, payload
        OP->>K: send(topic, orderId, envelope)

        alt Kafka publish succeeded
            OP->>OB: markSent(messageId)
            OB->>DB: UPDATE outbox_message<br/>status=SENT, published_at=now
        else Kafka publish failed
            OP->>OB: markFailed(messageId, retry metadata)
            OB->>DB: UPDATE outbox_message<br/>status=FAILED, retry_count++, next_retry_at, last_error
        end
    end

    opt M3+ design follow-up broker flow
        K->>BG: Deliver SubmitOrderCommand
        BG->>BG: Record broker binding, command attempt, wire journal
        BG->>B: Send ORDR TCP message including orderId
        B-->>BG: ACKN, reject, or timeout
        BG->>K: Publish canonical broker event
        K-->>AS: Deliver BrokerOrderAcknowledged or related event
        AS->>AS: Apply state machine transition<br/>LIVE, REJECTED, UNKNOWN, etc.
    end
```

### 핵심 포인트

* `clientOrderId`는 `order_instruction.client_instruction_id`로 저장되고, `accountId + PLACE + clientOrderId` 유니크 키로 멱등성을 보장한다.
* 동일 멱등성 키와 동일 payload는 기존 주문을 `200 OK`로 반환한다.
* 동일 멱등성 키와 다른 payload는 `409 Conflict`로 거절한다.
* 신규 주문은 `PENDING_ACK`, `cumQty = 0`, `leavesQty = orderQty`, `reconciliationStatus = NONE`으로 시작한다.
* `trade_order`, `order_instruction`, `order_event`, `outbox_message` 저장은 같은 DB transaction에 포함된다.
* 외부 브로커 호출은 API transaction 안에서 수행하지 않는다.
* `traceId`는 request header `X-Trace-Id`가 있으면 사용하고, 없으면 Order Service가 생성해 instruction과 outbox header, Kafka envelope까지 전파한다.

---

## 14.4 Broker Simulator M3 TCP 흐름

### 범위

이 diagram은 M3 구현 기준의 Broker Simulator 내부 흐름을 기록한다.
Broker Gateway는 아직 M4 이후 구현 범위이므로, 여기서는 TCP client 역할로만 표현한다.

M3 Broker Simulator는 다음 흐름을 제공한다.

* local/test profile 전용 admin API로 scenario와 in-memory state를 제어한다.
* TCP fixed-length frame을 Netty pipeline에서 분리한 뒤 `BrokerFrameCodec`으로 decode한다.
* `ORDR` 요청은 현재 scenario에 따라 `ACKN` 또는 `RJCT`로 응답한다.
* `OSTQ` 상태조회 요청은 in-memory 주문 상태를 `OSTS` snapshot으로 응답한다.
* duplicate fill admin API는 같은 논리 `FILL` frame을 같은 `wireMessageId`와 원 주문 `traceId`로 2회 전송한다.
* malformed frame/header/body는 Simulator 주문 상태를 변경하지 않고 log 후 connection close로 격리한다.

### Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant T as Test or Operator
    participant AC as SimulatorAdminController<br/>(local/test profile)
    participant ST as BrokerSimulatorState
    participant EP as BrokerSimulatorEventPublisher
    participant SS as BrokerSimulatorClientSessions
    participant C as TCP Client<br/>(Gateway in M4+)
    participant FD as BrokerSimulatorFrameDecoder
    participant TH as BrokerSimulatorTcpHandler
    participant CO as BrokerFrameCodec
    participant CH as Netty Channel

    opt Scenario setup
        T->>AC: PUT /api/simulator/scenario<br/>ACK_SUCCESS or REJECT_SUCCESS
        AC->>ST: setScenario(scenario)
        ST-->>AC: current scenario
        AC-->>T: ScenarioResponse
    end

    opt State reset
        T->>AC: POST /api/simulator/reset
        AC->>ST: reset()
        AC-->>T: 204 No Content
    end

    C->>FD: Send TCP bytes<br/>8-byte length + common header + body

    alt Malformed frame length
        FD-->>TH: BrokerSimulatorMalformedFrame(FRAME, reason)
        TH->>TH: log warn
        TH->>CH: close()
    else Complete frame
        FD-->>TH: byte[] frame
        TH->>CO: decode(frame)

        alt Malformed header or body
            CO-->>TH: BrokerParseResult.Malformed(type, reason)
            TH->>TH: log warn
            TH->>CH: close()
        else Parsed broker message
            alt ORDR decoded
                CO-->>TH: BrokerParseResult.Success(OrderRequest)

                alt Scenario == REJECT_SUCCESS
                    TH->>ST: reject(orderRequest)
                    ST-->>TH: SimulatorOrder(status=REJECTED)
                    TH->>TH: Build RJCT<br/>echo request wireMessageId<br/>preserve or generate traceId
                    TH->>CO: encode(OrderRejected)
                    CO-->>TH: byte[] responseFrame
                    TH->>CH: writeAndFlush(responseFrame)
                    TH->>SS: register(orderId, channel)
                    CH-->>C: RJCT TCP frame
                else Scenario == ACK_SUCCESS
                    TH->>ST: accept(orderRequest)
                    ST-->>TH: SimulatorOrder(status=ACCEPTED,<br/>brokerOrderId, traceId)
                    TH->>TH: Build ACKN<br/>echo request wireMessageId<br/>preserve or generate traceId
                    TH->>SS: register(orderId, channel)
                    TH->>CO: encode(OrderAccepted)
                    CO-->>TH: byte[] responseFrame
                    TH->>CH: writeAndFlush(responseFrame)
                    CH-->>C: ACKN TCP frame
                end
            else OSTQ decoded
                CO-->>TH: BrokerParseResult.Success(StatusQuery)
                TH->>ST: findForStatusQuery(orderId, brokerOrderId)

                alt Existing order
                    ST-->>TH: SimulatorOrder
                    TH->>TH: Build OSTS<br/>ACCEPTED or REJECTED
                else Unknown order
                    ST-->>TH: empty
                    TH->>TH: Build OSTS<br/>NOT_FOUND
                end

                TH->>CO: encode(StatusSnapshot)
                CO-->>TH: byte[] responseFrame
                TH->>CH: writeAndFlush(responseFrame)
                CH-->>C: OSTS TCP frame
            end
        end
    end

    opt Duplicate fill injection
        T->>AC: POST /api/simulator/orders/{orderId}/duplicate-fill
        AC->>EP: sendDuplicateFill(orderId)
        EP->>ST: findByOrderId(orderId)
        ST-->>EP: SimulatorOrder<br/>brokerOrderId, traceId, quantities
        EP->>SS: channelFor(orderId)
        SS-->>EP: active Netty channel
        EP->>EP: Build two FILL messages<br/>same wireMessageId, same traceId
        EP->>CO: encode(FillEvent 1)
        CO-->>EP: byte[] frame 1
        EP->>CH: writeAndFlush(frame 1)
        CH-->>C: FILL TCP frame
        EP->>CO: encode(FillEvent 2)
        CO-->>EP: byte[] frame 2
        EP->>CH: writeAndFlush(frame 2)
        CH-->>C: Duplicate FILL TCP frame
        EP-->>AC: DuplicateFillResult(wireMessageId, sentCount=2)
        AC-->>T: DuplicateFillResult
    end

    opt TCP channel closed
        CH-->>TH: channelInactive()
        TH->>SS: unregister(channel)
    end
```

### 핵심 포인트

* `ACK_SUCCESS`에서 `ACKN` frame 직렬화는 `handleOrderRequest()`의 `write(context, response)`가 공통 `write()`를 거쳐 `BrokerFrameCodec.encode(message)`를 호출하는 흐름이다.
* request/response correlation을 위해 `ACKN`, `RJCT`, `OSTS`는 요청의 `wireMessageId`를 echo한다.
* `traceId`는 요청 header에 있으면 보존하고, 없으면 Simulator가 `trace-simulator-{orderId}` 형식으로 생성한다.
* duplicate `FILL`은 원 주문의 `traceId`와 같은 `wireMessageId`를 재사용해 Gateway/Order Service의 dedup 검증 입력으로 사용한다.
* malformed 입력은 상태 저장소를 변경하지 않는다. 현재 M3 Simulator 정책은 WARN log 후 channel close이며, Gateway journal/metric 연결은 M4 이후 범위다.
