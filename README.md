# Trading Order Reliability Lab

외부 브로커와 비동기적으로 연동되는 주문 처리 시스템에서 **주문 상태의 신뢰성, 중복 이벤트 방어, 장애 복구, 상태 수렴** 문제를 다루는 Java/Spring 기반 실험 프로젝트입니다.

이 프로젝트는 실제 매매, 시세, 잔고, 정산, 손익 계산을 구현하는 것이 목적이 아닙니다.  
대신 주문 생성 이후 외부 브로커와의 통신 과정에서 발생할 수 있는 불확실성을 어떻게 다룰 것인지에 집중합니다.

## 문제의식

주문 시스템은 외부 시스템과 연결되는 순간 단순한 CRUD 문제가 아니게 됩니다.

예를 들어 다음과 같은 상황이 발생할 수 있습니다.

- 주문 요청은 보냈지만 브로커 응답이 timeout되는 경우
- 브로커가 같은 체결 이벤트를 중복 전송하는 경우
- 주문 접수 ACK보다 체결 이벤트가 먼저 도착하는 경우
- 취소 요청 중 추가 체결이 발생하는 경우
- 전문이 malformed되어 특정 주문에 귀속하기 어려운 경우
- 장 마감 이후에도 내부 주문이 non-terminal 상태로 남아 있는 경우

이 프로젝트는 이런 상황에서 주문 상태를 임의로 성공/실패 처리하지 않고, `UNKNOWN` 상태와 reconciliation 흐름을 통해 최종 상태로 수렴시키는 구조를 설계하고 구현합니다.

## 주요 목표

- 주문 상태를 단일 상태머신으로 일관되게 관리한다.
- 사용자 주문 생성/취소 요청을 멱등하게 처리한다.
- 외부 브로커 이벤트의 중복 수신과 순서 역전을 방어한다.
- 응답 timeout, 이벤트 유실, malformed 전문으로 인한 불확실성을 `UNKNOWN`으로 격리한다.
- 상태조회 기반 reconciliation으로 미확정 주문을 최종 상태로 수렴시킨다.
- Broker Gateway를 통해 브로커 통신과 주문 도메인 모델을 분리한다.
- Outbox / processed message 패턴을 통해 메시지 발행과 소비의 신뢰성을 보강한다.
- Netty 기반 Broker Simulator로 지연, 유실, 중복, 순서 역전, malformed 시나리오를 재현한다.

## 시스템 구성

**Order Service**
- 사용자-facing API
- 주문 상태 소유
- OrderInstruction 처리
- 주문 상태머신
- 주문 이벤트 이력
- SSE 기반 상태 알림

**Broker Gateway Service**
- 브로커 TCP 통신
- 전문 parsing / serialization
- broker command attempt 관리
- broker message journal 기록
- canonical broker event 변환

**Recovery Service**
- reconciliation job 관리
- 상태조회 command 발행
- retry / backoff 관리
- 복구 결과 추적

**Broker Simulator**
- Netty 기반 TCP 서버
- 브로커 측 주문 상태 시뮬레이션
- 접수/거절/체결/취소/만료/상태조회 응답
- 장애 시나리오 주입


## 프로젝트 범위

### 포함하는 것

* 해외주식 지정가 주문 생성
* 주문 생성 멱등성 처리
* 미체결 잔량 취소 instruction
* 부분체결 / 완전체결 / 취소 / 만료 상태 반영
* 응답 timeout 후 `UNKNOWN` 처리
* 상태조회 기반 reconciliation
* 중복 브로커 이벤트 방어
* 순서 역전 이벤트 처리
* malformed 전문 처리
* stale non-terminal 주문 탐지
* EOD 이후 DAY 주문 상태 확인
* Broker Gateway와 Broker Simulator 간 TCP 전문 통신

### 포함하지 않는 것

* 실제 매매 연동
* 실시간 시세
* 호가
* 계좌 원장
* 잔고 관리
* buying power
* position
* 평균 매입가
* 손익 계산
* 환전
* 정산
* 세금
* 수수료
* 실제 거래소 주문장 매칭

## 기술 스택

* Java 21
* Spring Boot
* Spring MVC
* Spring Data JPA
* MyBatis
* MySQL
* Kafka
* Netty
* Flyway
* Docker Compose
* Testcontainers
* Micrometer / Actuator

## 문서

상세 설계는 아래 문서에서 관리합니다.

| 단계 | 문서                                                          | 상태    |
| -: | ----------------------------------------------------------- | ----- |
|  1 | [프로젝트 문제 정의](docs/01-problem-definition.md)                 | 작성 완료 |
|  2 | [시스템 컨텍스트 다이어그램](docs/02-system-context.md)                 | 작성 완료 |
|  3 | [요구사항 정의](docs/03-requirements.md)                          | 작성 완료 |
|  4 | [도메인 모델과 상태 전이](docs/04-domain-model-and-state.md)          | 작성 완료 |
|  5 | [주요 유스케이스](docs/05-use-cases.md)                            | 작성 완료 |
|  6 | [품질 속성 시나리오](docs/06-quality-attribute-scenarios.md)        | 작성 완료 |
|  7 | [아키텍처 개요](docs/07-architecture-overview.md)                 | 작성 완료 |
|  8 | [ADR / DDR](docs/08-adr-ddr.md)                             | 작성 완료 |
|  9 | [DB 설계](docs/09-database-design.md)                         | 작성 완료 |
| 10 | [API / 이벤트 / 전문 명세](docs/10-api-event-protocol-spec.md)     | 작성 예정 |
| 10-A | [Broker TCP 전문 Byte-Level Layout](docs/10a-broker-tcp-byte-level-layout.md) | 작성 완료 |
| 11 | [장애 처리 / 재처리 정책](docs/11-failure-and-retry-policy.md)       | 작성 예정 |
| 12 | [테스트 / 모니터링 / 운영 계획](docs/12-test-monitoring-operations.md) | 작성 예정 |
| 13 | [개발 마일스톤](docs/13-development-milestones.md)                | 작성 예정 |

## 디렉토리 구조

초기 프로젝트는 monorepo multi-module 구조로 구성합니다.

```text
trading-order-reliability-lab/
  README.md
  settings.gradle.kts
  build.gradle.kts

  apps/
    order-service/
    broker-gateway-service/
    recovery-service/
    broker-simulator/

  libs/
    common-id/
    common-messaging/
    common-observability/
    broker-protocol/
    test-support/

  docs/
    01-problem-definition.md
    02-system-context.md
    03-requirements.md
    04-domain-model-and-state.md
    05-use-cases.md
    06-quality-attribute-scenarios.md
    07-architecture-overview.md
    08-adr-ddr.md
    09-database-design.md
    10-api-event-protocol-spec.md
    11-failure-and-retry-policy.md
    12-test-monitoring-operations.md
    13-development-milestones.md

  docker/
    docker-compose.yml
```
