package com.beewaxus.webhooksvcs.pubsrc.ledger;

public class IdempotencyLedgerException extends RuntimeException {
    public IdempotencyLedgerException(String message) {
        super(message);
    }

    public IdempotencyLedgerException(String message, Throwable cause) {
        super(message, cause);
    }
}

