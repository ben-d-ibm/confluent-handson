package com.bank.streaming.producer;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Domain value published to the loan-requests topic (serialized as Avro, see {@link LoanRequestAvro}).
 * Fields match the io.confluent.banking.LoanRequest schema consumed by Flink.
 * The producer never computes a score or a decision.
 */
public record LoanRequest(
        String requestId,
        int customerId,
        BigDecimal amount,
        int termMonths,
        String purpose,
        Instant requestedAt) {
}
