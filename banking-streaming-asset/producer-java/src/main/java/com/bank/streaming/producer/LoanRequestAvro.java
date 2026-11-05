package com.bank.streaming.producer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Maps a {@link LoanRequest} to a GenericRecord of the existing io.confluent.banking.LoanRequest schema.
 * The schema is NOT duplicated: terraform/schemas/loan-requests.avsc is packaged on the classpath at build time.
 */
public final class LoanRequestAvro {

    static final String SCHEMA_RESOURCE = "/loan-requests.avsc";
    public static final Schema SCHEMA = loadSchema();

    private static final Conversions.DecimalConversion DECIMAL = new Conversions.DecimalConversion();

    private LoanRequestAvro() {}

    public static GenericRecord toRecord(LoanRequest req) {
        Schema amountSchema = SCHEMA.getField("amount").schema();
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) amountSchema.getLogicalType();

        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("request_id", req.requestId());
        r.put("customer_id", req.customerId());
        // Throws AvroTypeException if scale != 2 or precision > 12 (no silent rounding).
        r.put("amount", DECIMAL.toBytes(req.amount(), amountSchema, decimalType));
        r.put("term_months", req.termMonths());
        r.put("purpose", req.purpose());
        r.put("requested_at", req.requestedAt().toEpochMilli());
        return r;
    }

    private static Schema loadSchema() {
        try (InputStream in = LoanRequestAvro.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Schema not found on classpath: " + SCHEMA_RESOURCE);
            }
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
