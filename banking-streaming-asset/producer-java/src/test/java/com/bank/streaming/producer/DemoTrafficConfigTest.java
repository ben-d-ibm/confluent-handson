package com.bank.streaming.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class DemoTrafficConfigTest {

    @Test
    void demoTrafficDefaultsToFalseAndAcceptsOnlyTrueOrFalse() {
        assertFalse(LoanRequestProducerApp.isDemoTraffic(Map.<String, String>of()::get));
        assertFalse(LoanRequestProducerApp.isDemoTraffic(Map.of("DEMO_TRAFFIC", "false")::get));
        assertTrue(LoanRequestProducerApp.isDemoTraffic(Map.of("DEMO_TRAFFIC", "true")::get));
        assertThrows(IllegalStateException.class,
                () -> LoanRequestProducerApp.isDemoTraffic(Map.of("DEMO_TRAFFIC", "yes")::get));
    }

    @Test
    void intervalDefaultsTo10SecondsAndRejectsInvalidValues() {
        assertEquals(10L, LoanRequestProducerApp.demoIntervalSeconds(Map.<String, String>of()::get));
        assertEquals(5L, LoanRequestProducerApp.demoIntervalSeconds(
                Map.of("DEMO_TRAFFIC_INTERVAL_SECONDS", "5")::get));
        for (String bad : new String[] {"0", "-3", "abc", "1.5"}) {
            assertThrows(IllegalStateException.class, () -> LoanRequestProducerApp.demoIntervalSeconds(
                    Map.of("DEMO_TRAFFIC_INTERVAL_SECONDS", bad)::get));
        }
    }
}
