package com.trading.orderreliability.order.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.orderreliability.order.adapter.out.persistence.JpaOrderInstructionRepository;
import com.trading.orderreliability.order.adapter.out.persistence.OrderInstructionRepository;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.InstructionType;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderInstruction;
import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.persistence.EntityManager;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class OrderApplicationServiceConcurrencyRegressionTest extends MySqlTestContainerSupport {

    private static final String ACCOUNT_ID = "ACC-CONCURRENT-001";
    private static final String CLIENT_ORDER_ID = "client-order-concurrent-race";
    private static final String CANCEL_ACCOUNT_ID = "ACC-CONCURRENT-CANCEL-001";
    private static final String CLIENT_CANCEL_REQUEST_ID = "client-cancel-concurrent-race";

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Test
    void 동일한_주문_생성_요청이_동시에_들어와도_하나의_주문으로_멱등하게_수렴한다() throws Exception {
        // given: 테스트 전용 repository가 두 요청 모두 기존 PLACE instruction이 없다고 읽은 뒤에만
        // createOrder 흐름을 계속 진행시킨다. 즉, 실제 운영에서 발생할 수 있는 동시 재시도 race를 고정한다.
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Callable<PlaceOrderResult> request = () -> orderApplicationService.createOrder(placeCommand());

            // when: 같은 계좌, 같은 clientOrderId, 같은 payload를 가진 주문 생성 요청 두 개가 동시에 들어온다.
            List<Future<PlaceOrderResult>> futures = executorService.invokeAll(List.of(request, request));
            List<PlaceOrderResult> results = futures.stream()
                    .map(OrderApplicationServiceConcurrencyRegressionTest::getResult)
                    .toList();

            // then: 한 요청만 신규 생성으로 처리되고, 다른 요청은 DB 예외가 아니라 기존 주문 결과로 수렴해야 한다.
            assertThat(results).hasSize(2);
            assertThat(results.stream().filter(PlaceOrderResult::created)).hasSize(1);
            assertThat(results.stream().map(result -> result.order().orderId()).distinct()).hasSize(1);

            // then: unique 충돌을 흡수하는 과정에서 orphan trade_order가 커밋되면 안 된다.
            assertThat(orderApplicationService.listOrders(ACCOUNT_ID, null, 10)).hasSize(1);
        } finally {
            executorService.shutdownNow();
            assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void 같은_계좌의_동일한_취소_멱등키가_서로_다른_주문에_동시에_사용되면_도메인_충돌로_수렴한다() throws Exception {
        // given: 같은 계좌의 서로 다른 주문 두 건을 준비한다.
        Order firstOrder = orderApplicationService.createOrder(placeCommand(
                "client-order-concurrent-cancel-001",
                "AAPL",
                CANCEL_ACCOUNT_ID
        )).order();
        Order secondOrder = orderApplicationService.createOrder(placeCommand(
                "client-order-concurrent-cancel-002",
                "MSFT",
                CANCEL_ACCOUNT_ID
        )).order();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Callable<Object> firstCancel = () -> cancelOrException(firstOrder, "trace-cancel-concurrent-001");
            Callable<Object> secondCancel = () -> cancelOrException(secondOrder, "trace-cancel-concurrent-002");

            // when: 두 주문에 같은 clientCancelRequestId를 동시에 사용한다.
            List<Object> results = executorService.invokeAll(List.of(firstCancel, secondCancel))
                    .stream()
                    .map(OrderApplicationServiceConcurrencyRegressionTest::getObjectResult)
                    .toList();

            // then: 하나만 취소 접수되고, 다른 하나는 DB 예외가 아니라 도메인 멱등성 충돌로 정리되어야 한다.
            assertThat(results.stream().filter(CancelOrderResult.class::isInstance)).hasSize(1);
            assertThat(results.stream().filter(IdempotencyConflictException.class::isInstance)).hasSize(1);
            assertThat(orderApplicationService.getOrder(firstOrder.orderId().value()).status() == OrderStatus.PENDING_CANCEL
                    || orderApplicationService.getOrder(secondOrder.orderId().value()).status() == OrderStatus.PENDING_CANCEL)
                    .isTrue();
            assertThat(orderApplicationService.getOrder(firstOrder.orderId().value()).status() == OrderStatus.PENDING_ACK
                    || orderApplicationService.getOrder(secondOrder.orderId().value()).status() == OrderStatus.PENDING_ACK)
                    .isTrue();
        } finally {
            executorService.shutdownNow();
            assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static PlaceOrderResult getResult(Future<PlaceOrderResult> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("동시 주문 생성 테스트가 인터럽트되었습니다.", e);
        } catch (ExecutionException e) {
            throw new AssertionError("동시 주문 생성 요청은 예외 없이 멱등 결과로 수렴해야 합니다.", e.getCause());
        }
    }

    private static Object getObjectResult(Future<Object> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("동시 취소 요청 테스트가 인터럽트되었습니다.", e);
        } catch (ExecutionException e) {
            throw new AssertionError("동시 취소 요청 callable은 예외를 결과로 반환해야 합니다.", e.getCause());
        }
    }

    private static PlaceOrderCommand placeCommand() {
        return placeCommand(CLIENT_ORDER_ID, "AAPL", ACCOUNT_ID);
    }

    private static PlaceOrderCommand placeCommand(String clientOrderId, String symbol, String accountId) {
        return new PlaceOrderCommand(
                clientOrderId,
                new AccountId(accountId),
                Market.US,
                new Symbol(symbol),
                OrderSide.BUY,
                OrderType.LIMIT,
                TimeInForce.DAY,
                OrderQuantity.positive(100),
                new OrderPrice(new BigDecimal("189.50")),
                "trace-" + UUID.randomUUID()
        );
    }

    private Object cancelOrException(Order order, String traceId) {
        try {
            return orderApplicationService.cancelOrder(
                    order.orderId().value(),
                    new CancelOrderCommand(new AccountId(CANCEL_ACCOUNT_ID), CLIENT_CANCEL_REQUEST_ID, traceId)
            );
        } catch (RuntimeException exception) {
            return exception;
        }
    }

    @TestConfiguration
    static class ConcurrencyRaceTestConfiguration {

        @Bean
        @Primary
        OrderInstructionRepository raceGateOrderInstructionRepository(
                JpaOrderInstructionRepository jpaRepository,
                EntityManager entityManager
        ) {
            return new RaceGateOrderInstructionRepository(jpaRepository, entityManager);
        }
    }

    static class RaceGateOrderInstructionRepository extends OrderInstructionRepository {

        private final CountDownLatch bothRequestsReadEmptyInstruction = new CountDownLatch(2);
        private final CountDownLatch bothCancelRequestsReadEmptyInstruction = new CountDownLatch(2);

        RaceGateOrderInstructionRepository(JpaOrderInstructionRepository jpaRepository, EntityManager entityManager) {
            super(jpaRepository, entityManager);
        }

        @Override
        public Optional<OrderInstruction> findByIdempotencyKey(String accountId, InstructionType instructionType, String clientInstructionId) {
            Optional<OrderInstruction> result = super.findByIdempotencyKey(accountId, instructionType, clientInstructionId);
            if (shouldSynchronizeEmptyPlaceRead(accountId, instructionType, clientInstructionId, result)) {
                awaitOtherConcurrentRequest();
            }
            if (shouldSynchronizeEmptyCancelRead(accountId, instructionType, clientInstructionId, result)) {
                awaitOtherConcurrentCancelRequest();
            }
            return result;
        }

        private static boolean shouldSynchronizeEmptyPlaceRead(
                String accountId,
                InstructionType instructionType,
                String clientInstructionId,
                Optional<OrderInstruction> result
        ) {
            return result.isEmpty()
                    && ACCOUNT_ID.equals(accountId)
                    && InstructionType.PLACE == instructionType
                    && CLIENT_ORDER_ID.equals(clientInstructionId);
        }

        private static boolean shouldSynchronizeEmptyCancelRead(
                String accountId,
                InstructionType instructionType,
                String clientInstructionId,
                Optional<OrderInstruction> result
        ) {
            return result.isEmpty()
                    && CANCEL_ACCOUNT_ID.equals(accountId)
                    && InstructionType.CANCEL == instructionType
                    && CLIENT_CANCEL_REQUEST_ID.equals(clientInstructionId);
        }

        private void awaitOtherConcurrentRequest() {
            bothRequestsReadEmptyInstruction.countDown();
            try {
                if (!bothRequestsReadEmptyInstruction.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("두 동시 요청이 모두 기존 instruction 없음 상태를 읽지 못했습니다.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("동시 주문 생성 race 고정 중 인터럽트되었습니다.", e);
            }
        }

        private void awaitOtherConcurrentCancelRequest() {
            bothCancelRequestsReadEmptyInstruction.countDown();
            try {
                if (!bothCancelRequestsReadEmptyInstruction.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("두 동시 취소 요청이 모두 기존 instruction 없음 상태를 읽지 못했습니다.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("동시 취소 요청 race 고정 중 인터럽트되었습니다.", e);
            }
        }
    }
}
