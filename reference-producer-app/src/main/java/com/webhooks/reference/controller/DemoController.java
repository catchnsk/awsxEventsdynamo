package com.webhooks.reference.controller;

import com.webhooks.reference.model.CustomerUpdateRequest;
import com.webhooks.reference.service.DemoEventService;
import com.webhooks.sdk.core.PublishResponse;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/demo")
public class DemoController {

    private final DemoEventService demoEventService;

    public DemoController(DemoEventService demoEventService) {
        this.demoEventService = demoEventService;
    }

    @PostMapping("/customers/{customerId}")
    public ResponseEntity<PublishResponse> updateCustomer(@PathVariable String customerId,
                                                          @Valid @RequestBody CustomerUpdateRequest request) {
        PublishResponse response = demoEventService.publishCustomerUpdate(customerId, request);
        return ResponseEntity.accepted().body(response);
    }
}
