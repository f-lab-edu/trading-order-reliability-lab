package com.trading.orderreliability.simulator.web;

import com.trading.orderreliability.simulator.domain.BrokerSimulatorEventPublisher;
import com.trading.orderreliability.simulator.domain.BrokerSimulatorState;
import com.trading.orderreliability.simulator.domain.SimulatorOrder;
import com.trading.orderreliability.simulator.domain.SimulatorScenario;

import org.springframework.http.HttpStatus;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

@RestController
@Profile({"local", "test"})
@RequestMapping("/api/simulator")
public class SimulatorAdminController {

    private final BrokerSimulatorState state;
    private final BrokerSimulatorEventPublisher eventPublisher;

    public SimulatorAdminController(BrokerSimulatorState state, BrokerSimulatorEventPublisher eventPublisher) {
        this.state = state;
        this.eventPublisher = eventPublisher;
    }

    @PutMapping("/scenario")
    public ScenarioResponse setScenario(@RequestBody ScenarioRequest request) {
        SimulatorScenario scenario = state.setScenario(request.scenario());
        return new ScenarioResponse(scenario);
    }

    @PostMapping("/reset")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void reset() {
        state.reset();
    }

    @GetMapping("/orders")
    public List<SimulatorOrder> orders() {
        return state.orders();
    }

    @GetMapping("/orders/{orderId}")
    public SimulatorOrder order(@PathVariable UUID orderId) {
        return state.findByOrderId(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "simulator order not found"));
    }

    @PostMapping("/orders/{orderId}/duplicate-fill")
    public BrokerSimulatorEventPublisher.DuplicateFillResult duplicateFill(@PathVariable UUID orderId) {
        return eventPublisher.sendDuplicateFill(orderId);
    }

    @PostMapping("/orders/{orderId}/fills")
    public BrokerSimulatorEventPublisher.FillResult fill(
            @PathVariable UUID orderId,
            @RequestBody FillRequest request
    ) {
        if (request == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "request body must not be null");
        }
        return eventPublisher.sendFill(orderId, request.lastFillQty());
    }

    public record ScenarioRequest(SimulatorScenario scenario) {
    }

    public record ScenarioResponse(SimulatorScenario scenario) {
    }

    public record FillRequest(long lastFillQty) {
    }
}
