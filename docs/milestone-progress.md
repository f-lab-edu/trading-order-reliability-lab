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
| 마지막 갱신 | 2026-06-11 |
| 현재 active milestone | `M2` Reliable messaging 기반 |
| M2 상태 | 구현 및 reviewer/design_guardian 피드백 루프 완료 |
| 다음 권장 작업 | M2 변경분 최종 점검 후 커밋/PR 또는 M3 착수 |
| 최신 작업 로그 | `local-notes/ai-work-log/2026-06-10-M2.md` |
| 최신 전체 검증 | `./gradlew --gradle-user-home .gradle test` 성공 |

---

## Milestone별 진행 상태

| Milestone | 상태 | 근거 / 비고 |
| --- | --- | --- |
| `M0` 프로젝트 기반 | 완료된 기반으로 취급 | 현재 multi-module, service app, Gradle/Docker/Test 기반 위에서 M1/M2 작업 진행 중 |
| `M1` Order 도메인 코어 | 완료된 기반으로 취급 | 주문 생성/취소 skeleton, 상태 전이, DB/API 기반 위에서 M2 outbox 연동 완료 |
| `M2` Reliable messaging 기반 | 구현 완료, 최종 정리 단계 | outbox, publisher, processed guard, envelope, traceId, 최소 parking log 구현 및 리뷰 루프 완료 |
| `M3` Broker protocol과 Simulator | 미착수 | M2 최종 커밋/정리 이후 착수 |

상태 표현 기준:

* `미착수`: 설계만 있고 구현 thread가 시작되지 않음
* `진행 중`: 구현 또는 리뷰 피드백 반영 중
* `구현 완료, 최종 정리 단계`: 주요 구현과 검증은 끝났으나 커밋/PR/문서 동기화 확인이 남음
* `완료`: 커밋/PR 또는 사용자 승인 기준으로 milestone 종료 처리됨

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

1. 요청이 M2 messaging 기반의 마무리, 커밋, 리뷰, 문서 보강이면 `M2`를 active milestone로 유지한다.
2. 요청이 broker protocol, TCP frame, simulator라면 `M3` 착수로 판단한다.
3. 요청이 정상 주문 end-to-end Kafka consumer 연결이라면 `M4` 선행 조건으로 M3 완료 여부를 먼저 확인한다.
4. 요청이 parking consumer handler 완성, 반복 실패 5회 후 parking, malformed/stale/EOD hardening이면 `M10` 범위일 수 있으므로 M2 작업으로 끌어오지 않는다.

작업 후에는 이 문서의 "현재 상태 요약", "Milestone별 진행 상태", "다음 thread가 반드시 확인할 위험"을 갱신한다.
