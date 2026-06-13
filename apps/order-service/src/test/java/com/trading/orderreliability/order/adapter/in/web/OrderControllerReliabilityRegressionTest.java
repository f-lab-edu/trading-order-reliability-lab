package com.trading.orderreliability.order.adapter.in.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("주문 컨트롤러 신뢰성 회귀")
class OrderControllerReliabilityRegressionTest extends MySqlTestContainerSupport {

    @Autowired
    private WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @BeforeEach
    void MockMvc를_웹_애플리케이션_컨텍스트로_구성한다() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    @DisplayName("주문 생성 재시도는 X-Trace-Id가 없어서 서버가 새 traceId를 만들어도 같은 주문을 반환해야 한다")
    void 주문_생성_재시도는_traceId가_바뀌어도_같은_주문을_반환한다() throws Exception {
        // given: 클라이언트가 같은 주문 생성 payload를 준비하되 X-Trace-Id 헤더는 보내지 않는다.
        // 서버가 요청마다 새 traceId를 만들더라도 traceId는 멱등성 hash 대상이 아니어야 한다.
        String requestBody = """
                {
                  "clientOrderId": "controller-client-order-001",
                  "accountId": "ACC-CONTROLLER-001",
                  "market": "US",
                  "symbol": "AAPL",
                  "side": "BUY",
                  "orderType": "LIMIT",
                  "tif": "DAY",
                  "orderQty": 100,
                  "limitPrice": "189.50"
                }
                """;

        // when: 첫 요청은 새 주문을 만들고, 같은 payload의 두 번째 요청은 재시도로 처리한다.
        MvcResult firstResult = mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isCreated())
                .andReturn();
        String firstOrderId = JsonPath.read(firstResult.getResponse().getContentAsString(), "$.orderId");

        MvcResult secondResult = mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andReturn();
        String secondOrderId = JsonPath.read(secondResult.getResponse().getContentAsString(), "$.orderId");

        // then: 재시도 응답은 409가 아니라 기존 주문 ID를 그대로 반환해야 한다.
        assertThat(secondOrderId).isEqualTo(firstOrderId);
    }

    @Test
    @DisplayName("취소 요청 재시도는 X-Trace-Id가 없어서 서버가 새 traceId를 만들어도 기존 취소 요청을 반환해야 한다")
    void 취소_요청_재시도는_traceId가_바뀌어도_기존_취소_요청을_반환한다() throws Exception {
        // given: 취소 대상 주문을 하나 생성한다. 주문 생성 traceId는 이 테스트의 관심사가 아니므로 명시한다.
        MvcResult createResult = mockMvc.perform(post("/api/orders")
                        .header("X-Trace-Id", "trace-create-controller-cancel-001")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "clientOrderId": "controller-client-order-002",
                                  "accountId": "ACC-CONTROLLER-002",
                                  "market": "US",
                                  "symbol": "MSFT",
                                  "side": "BUY",
                                  "orderType": "LIMIT",
                                  "tif": "DAY",
                                  "orderQty": 100,
                                  "limitPrice": "250.00"
                                }
                                """))
                .andExpect(status().isCreated())
                .andReturn();
        String orderId = JsonPath.read(createResult.getResponse().getContentAsString(), "$.orderId");

        String cancelBody = """
                {
                  "accountId": "ACC-CONTROLLER-002",
                  "clientCancelRequestId": "controller-cancel-001"
                }
                """;

        // when: 같은 취소 payload를 X-Trace-Id 없이 두 번 보낸다.
        mockMvc.perform(post("/api/orders/{orderId}/cancellations", UUID.fromString(orderId))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(cancelBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clientCancelRequestId").value("controller-cancel-001"));

        // then: 두 번째 요청은 traceId 차이 때문에 409가 나면 안 되고 기존 취소 요청 결과를 반환해야 한다.
        mockMvc.perform(post("/api/orders/{orderId}/cancellations", UUID.fromString(orderId))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(cancelBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clientCancelRequestId").value("controller-cancel-001"))
                .andExpect(jsonPath("$.orderStatus").value("PENDING_CANCEL"));
    }

    @Test
    @DisplayName("주문 생성 필수 enum 필드가 누락되면 서버 오류가 아니라 400 Bad Request로 응답해야 한다")
    void 주문_생성_필수_enum_필드_누락은_400으로_응답한다() throws Exception {
        // given: market은 필수 필드다. 누락된 필수 필드는 도메인 생성 중 NPE로 새면 안 된다.
        String requestBody = """
                {
                  "clientOrderId": "controller-client-order-003",
                  "accountId": "ACC-CONTROLLER-003",
                  "symbol": "NVDA",
                  "side": "BUY",
                  "orderType": "LIMIT",
                  "tif": "DAY",
                  "orderQty": 100,
                  "limitPrice": "900.00"
                }
                """;

        // when/then: API 경계에서 잘못된 요청으로 분류하고 일관된 에러 코드를 내려야 한다.
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    @DisplayName("주문 생성 필수 가격 필드가 누락되면 서버 오류가 아니라 400 Bad Request로 응답해야 한다")
    void 주문_생성_필수_가격_필드_누락은_400으로_응답한다() throws Exception {
        // given: limitPrice는 필수 필드다. null 가격은 검증 오류로 처리되어야 한다.
        String requestBody = """
                {
                  "clientOrderId": "controller-client-order-004",
                  "accountId": "ACC-CONTROLLER-004",
                  "market": "US",
                  "symbol": "TSLA",
                  "side": "BUY",
                  "orderType": "LIMIT",
                  "tif": "DAY",
                  "orderQty": 100
                }
                """;

        // when/then: null 필드가 NPE로 전파되지 않고 400 응답으로 정리되는지 확인한다.
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    @DisplayName("주문 생성 요청 body가 JSON null이면 서버 오류가 아니라 400 Bad Request로 응답해야 한다")
    void 주문_생성_body_null은_400으로_응답한다() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("null"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    @DisplayName("주문 생성 멱등키가 DB 컬럼 길이를 넘으면 400 Bad Request로 응답해야 한다")
    void 주문_생성_멱등키_길이_초과는_400으로_응답한다() throws Exception {
        String requestBody = """
                {
                  "clientOrderId": "%s",
                  "accountId": "ACC-CONTROLLER-005",
                  "market": "US",
                  "symbol": "AAPL",
                  "side": "BUY",
                  "orderType": "LIMIT",
                  "tif": "DAY",
                  "orderQty": 100,
                  "limitPrice": "189.50"
                }
                """.formatted("x".repeat(65));

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    @DisplayName("Trace ID가 DB 컬럼 길이를 넘으면 400 Bad Request로 응답해야 한다")
    void trace_id_길이_초과는_400으로_응답한다() throws Exception {
        String requestBody = """
                {
                  "clientOrderId": "controller-client-order-005",
                  "accountId": "ACC-CONTROLLER-006",
                  "market": "US",
                  "symbol": "AAPL",
                  "side": "BUY",
                  "orderType": "LIMIT",
                  "tif": "DAY",
                  "orderQty": 100,
                  "limitPrice": "189.50"
                }
                """;

        mockMvc.perform(post("/api/orders")
                        .header("X-Trace-Id", "t".repeat(65))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    @DisplayName("취소 요청 body가 JSON null이면 서버 오류가 아니라 400 Bad Request로 응답해야 한다")
    void 취소_요청_body_null은_400으로_응답한다() throws Exception {
        mockMvc.perform(post("/api/orders/{orderId}/cancellations", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("null"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }
}
