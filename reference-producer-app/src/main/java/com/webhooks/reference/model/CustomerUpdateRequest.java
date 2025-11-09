package com.webhooks.reference.model;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record CustomerUpdateRequest(
        @NotBlank String status,
        @Email String email,
        Map<String, String> headers
) {
    public CustomerUpdateRequest {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
