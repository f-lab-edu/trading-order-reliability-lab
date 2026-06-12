# 00. 설계문서 인덱스

이 문서는 Codex와 개발자가 마일스톤 작업을 시작할 때 먼저 읽는 진입점이다.
기존 설계문서 구조를 유지하며, 각 작업은 아래 문서 중 관련 항목을 확인한 뒤 진행한다.

## 반드시 지켜야 할 설계 Invariant

1. 주문 상태 변경은 추적 가능한 이벤트 또는 기록으로 설명 가능해야 한다.
2. `clientOrderId` 기반 주문 요청 멱등성을 보존한다.
3. `brokerOrderId`, `wireMessageId`, `traceId` 추적성을 보존한다.
4. 브로커 응답 불확실 상태는 `UNKNOWN` 또는 reconciliation 경로로 수렴시킨다.
5. malformed broker message는 상태를 임의 변경하지 않고 journal/log/recovery 경로로 보낸다.
6. outbox / processed message 기반 at-least-once + idempotent processing을 유지한다.
7. 부분체결 후 취소는 체결분 확정과 미체결 잔량 취소 정책을 따른다.
8. public API, DB schema, event contract, broker wire protocol 변경은 호환성 영향을 기록한다.

## 문서 목록

- `docs/milestone-progress.md`: 현재 milestone 진행 상태, 다음 작업 진입점, 최근 검증 결과
- `docs/01-problem-definition.md`: 문제 정의와 프로젝트 배경
- `docs/02-system-context.md`: 시스템 컨텍스트와 외부 경계
- `docs/03-requirements.md`: 기능/비기능 요구사항
- `docs/04-domain-model-and-state.md`: 주문 도메인 모델과 상태 전이
- `docs/05-use-cases.md`: 주요 유스케이스
- `docs/06-quality-attribute-scenarios.md`: 품질 속성 시나리오
- `docs/07-architecture-overview.md`: 아키텍처 개요
- `docs/08-adr-ddr.md`: ADR/DDR 의사결정 기록
- `docs/09-database-design.md`: DB 설계
- `docs/10-api-event-protocol-spec.md`: API/Event/Protocol 설계
- `docs/10a-broker-tcp-byte-level-layout.md`: Broker TCP byte-level layout
- `docs/11-failure-and-retry-policy.md`: 장애/재시도/recovery 정책
- `docs/12-test-monitoring-operations.md`: 테스트/모니터링/운영 계획
- `docs/13-development-milestones.md`: 개발 마일스톤

## 마일스톤 작업 기본 흐름

1. 이 문서를 읽고 관련 설계문서를 고른다.
2. `docs/milestone-progress.md`에서 현재 active milestone, 완료/보류 항목, 다음 handoff를 확인한다.
3. `docs/13-development-milestones.md`에서 현재 milestone의 완료 기준을 확인한다.
4. 구현 전에 public contract, DB schema, event/wire protocol 영향 여부를 판단한다.
5. 구현 후 테스트와 read-only review를 수행한다.
6. 작업 과정 로그와 커리어 후보 로그는 main workspace의 `local-notes/ai-work-log/`, `local-notes/career/`에 한국어로 갱신한다.
