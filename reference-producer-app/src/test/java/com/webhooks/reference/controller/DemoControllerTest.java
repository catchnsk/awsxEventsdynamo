package com.webhooks.reference.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhooks.reference.model.CustomerUpdateRequest;
import com.webhooks.reference.service.DemoEventService;
import com.webhooks.sdk.core.PublishResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = DemoController.class)
class DemoControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DemoEventService demoEventService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void publishesCustomerUpdate() throws Exception {
        Mockito.when(demoEventService.publishCustomerUpdate(eq("123"), any(CustomerUpdateRequest.class)))
                .thenReturn(new PublishResponse("event-123", true, Instant.now(), Duration.ofMillis(10)));

        mockMvc.perform(post("/demo/customers/123")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"ACTIVE\",\"email\":\"demo@example.com\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value("event-123"));
    }
}
