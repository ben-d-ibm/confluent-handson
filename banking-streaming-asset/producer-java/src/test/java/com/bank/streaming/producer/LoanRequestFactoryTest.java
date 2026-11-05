package com.bank.streaming.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class LoanRequestFactoryTest {

    private static final Instant NOW = Instant.parse("2025-01-01T00:00:00.123456Z");

    private final LoanRequestFactory factory =
            new LoanRequestFactory(new Random(42), Clock.fixed(NOW, ZoneOffset.UTC), 50);

    @Test
    void requestIdsAreUnique() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            assertTrue(ids.add(factory.next().requestId()));
        }
    }

    @Test
    void schemaIsTheTerraformOneNotACopy() throws Exception {
        Schema tf = new Schema.Parser().parse(Files.readString(Path.of("../terraform/schemas/loan-requests.avsc")));
        assertEquals(tf, LoanRequestAvro.SCHEMA);
        assertEquals("io.confluent.banking.LoanRequest", LoanRequestAvro.SCHEMA.getFullName());
    }

    @Test
    void recordContainsExactlyTheSchemaFields() {
        GenericRecord r = LoanRequestAvro.toRecord(factory.next());
        Set<String> names = r.getSchema().getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
        assertEquals(Set.of("request_id", "customer_id", "amount", "term_months", "purpose", "requested_at"), names);
        assertTrue(GenericData.get().validate(LoanRequestAvro.SCHEMA, r));
    }

    @Test
    void customerIdIsAvroIntInSeededRange() {
        for (int i = 0; i < 200; i++) {
            LoanRequest req = factory.next();
            assertTrue(req.customerId() >= 1001 && req.customerId() <= 1050);
            assertInstanceOf(Integer.class, LoanRequestAvro.toRecord(req).get("customer_id"));
        }
    }

    @Test
    void amountIsDecimalBytesWithScale2() {
        LoanRequest req = factory.next();
        GenericRecord r = LoanRequestAvro.toRecord(req);
        Schema amountSchema = LoanRequestAvro.SCHEMA.getField("amount").schema();
        LogicalTypes.Decimal dt = (LogicalTypes.Decimal) amountSchema.getLogicalType();
        assertEquals(12, dt.getPrecision());
        assertEquals(2, dt.getScale());
        BigDecimal decoded = new Conversions.DecimalConversion()
                .fromBytes((ByteBuffer) r.get("amount"), amountSchema, dt);
        assertEquals(2, decoded.scale());
        assertEquals(req.amount(), decoded);
    }

    @Test
    void amountWithWrongScaleIsRejectedNotRounded() {
        LoanRequest bad = new LoanRequest("id", 1001, new BigDecimal("10.123"), 12, "HOME", NOW);
        assertThrows(AvroTypeException.class, () -> LoanRequestAvro.toRecord(bad));
    }

    @Test
    void requestedAtIsEpochMillis() {
        LoanRequest req = factory.next();
        Object v = LoanRequestAvro.toRecord(req).get("requested_at");
        assertInstanceOf(Long.class, v);
        assertEquals(NOW.toEpochMilli(), v);
        assertEquals(1735689600123L, v);
    }

    @Test
    void valueSerializerIsAvroNeverJson() {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP_SERVERS", "broker:9092");
        env.put("PRODUCER_KAFKA_API_KEY", "k");
        env.put("PRODUCER_KAFKA_API_SECRET", "s");
        env.put("SCHEMA_REGISTRY_URL", "https://sr.example");
        env.put("PRODUCER_SR_API_KEY", "srk");
        env.put("PRODUCER_SR_API_SECRET", "srs");

        Properties p = LoanRequestProducerApp.buildConfig(env::get);

        assertEquals(KafkaAvroSerializer.class.getName(), p.get("value.serializer"));
        assertEquals(StringSerializer.class.getName(), p.get("key.serializer"));
        assertEquals("false", p.get("auto.register.schemas"));
        assertEquals("true", p.get("use.latest.version"));
        assertEquals("USER_INFO", p.get("basic.auth.credentials.source"));
        assertEquals("srk:srs", p.get("basic.auth.user.info"));
        assertEquals("https://sr.example", p.get("schema.registry.url"));
    }

    @Test
    void fixedRequestIsOffByDefaultAndHonoursOverrides() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        assertEquals(null, LoanRequestProducerApp.fixedRequest(k -> null, clock));

        Map<String, String> env = Map.of("LOAN_CUSTOMER_ID", "1001");
        LoanRequest a = LoanRequestProducerApp.fixedRequest(env::get, clock);
        LoanRequest b = LoanRequestProducerApp.fixedRequest(env::get, clock);
        assertEquals(1001, a.customerId());
        assertEquals(new BigDecimal("10000.00"), a.amount());
        assertEquals(24, a.termMonths());
        assertEquals("PERSONAL", a.purpose());
        assertTrue(!a.requestId().equals(b.requestId()));
        assertTrue(GenericData.get().validate(LoanRequestAvro.SCHEMA, LoanRequestAvro.toRecord(a)));
    }

    @Test
    void missingSchemaRegistryVariableFailsFast() {
        assertThrows(IllegalStateException.class, () -> LoanRequestProducerApp.buildConfig(k -> null));
    }
}
