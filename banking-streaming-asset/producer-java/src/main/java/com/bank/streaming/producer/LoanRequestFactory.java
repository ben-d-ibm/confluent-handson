package com.bank.streaming.producer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Generates synthetic loan requests (raw input only, no scoring). */
public class LoanRequestFactory {

    static final List<String> PURPOSES =
            List.of("HOME", "CAR", "EDUCATION", "PERSONAL", "BUSINESS", "DEBT_CONSOLIDATION");
    static final int[] TERMS = {12, 24, 36, 48, 60, 120, 240};
    /** First customer_id seeded in postgres/init/02-seed.sql (1001-1050). */
    static final int FIRST_CUSTOMER_ID = 1001;

    private final Random random;
    private final Clock clock;
    private final int customerPool;

    public LoanRequestFactory(Random random, Clock clock, int customerPool) {
        this.random = random;
        this.clock = clock;
        this.customerPool = customerPool;
    }

    public LoanRequest next() {
        int customerId = FIRST_CUSTOMER_ID + random.nextInt(customerPool);
        BigDecimal amount = BigDecimal.valueOf(1_000 + random.nextInt(99_000))
                .setScale(2, RoundingMode.HALF_UP);
        int term = TERMS[random.nextInt(TERMS.length)];
        String purpose = PURPOSES.get(random.nextInt(PURPOSES.size()));
        return new LoanRequest(
                UUID.randomUUID().toString(),
                customerId,
                amount,
                term,
                purpose,
                Instant.now(clock).truncatedTo(ChronoUnit.MILLIS));
    }
}
