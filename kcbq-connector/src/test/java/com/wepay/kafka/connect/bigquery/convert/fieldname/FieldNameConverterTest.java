package com.wepay.kafka.connect.bigquery.convert.fieldname;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FieldNameConverterTest {

    private static final Boolean SHOULD_CONVERT_DOUBLE = true;

    @Test
    public void testRecordConvertIntyyyyMMddToDate() {

        final String fieldNameUTC = "DayIDUTC";
        final Long fieldValueUTC = 20191217L;
        final Integer fieldValueUTCint = 20191217;
        final String expectedValueUTC = "2019-12-17";

        final String fieldNameEST = "DayID";
        final Long fieldValueEST = 20191211L;
        final Integer fieldValueESTint = 20191211;
        final String expectedValueEST = "2019-12-11";

        final Map<String,String> fieldNameMap = new HashMap<String, String>() {{
            put(fieldNameUTC, FieldNameConverters.IntyyyyMMddToDate.CONVERTER_NAME);
            put(fieldNameEST, FieldNameConverters.IntyyyyMMddToDate.CONVERTER_NAME);
        }};

        testRecordConvert(fieldNameUTC, fieldValueUTC, Schema.INT64_SCHEMA, expectedValueUTC, fieldNameMap);
        testRecordConvert(fieldNameEST, fieldValueEST, Schema.INT64_SCHEMA, expectedValueEST, fieldNameMap);
        testRecordConvert(fieldNameUTC, fieldValueUTCint, Schema.INT32_SCHEMA, expectedValueUTC, fieldNameMap);
        testRecordConvert(fieldNameEST, fieldValueESTint, Schema.INT32_SCHEMA, expectedValueEST, fieldNameMap);
    }

    @Test
    public void testSchemaConvertIntyyyyMMddToDate() {
        final String fieldName = "DayID";

        testSchemaConvert(fieldName, Schema.INT32_SCHEMA,
                new FieldNameConverters.IntyyyyMMddToDate(), LegacySQLTypeName.DATE);
        testSchemaConvert(fieldName, Schema.INT64_SCHEMA,
                new FieldNameConverters.IntyyyyMMddToDate(), LegacySQLTypeName.DATE);
    }

    @Test
    public void testRecordConvertNumberInSecondsToTimestamp() {

        final String fieldNameUTC = "CreatedOnUTC";
        final long fieldValueUTC = 1576078615L;
        final int fieldValueUTCint = 1576078615;
        final String expectedValueUTC = "2019-12-11 15:36:55.000Z";

        final String fieldNameEST = "CreatedOnEST";
        final long fieldValueEST = 1576078615L;
        final int fieldValueESTint = 1576078615;
        final String expectedValueEST = "2019-12-11 15:36:55.000-05:00";

        final Map<String,String> fieldNameMap = new HashMap<String, String>() {{
            put(fieldNameUTC, FieldNameConverters.NumberInSecondsToTimestampUTC.CONVERTER_NAME);
            put(fieldNameEST, FieldNameConverters.NumberInSecondsToTimestampEST.CONVERTER_NAME);
        }};

        testRecordConvert(fieldNameUTC, fieldValueUTC, Schema.INT64_SCHEMA, expectedValueUTC, fieldNameMap);
        testRecordConvert(fieldNameEST, fieldValueEST, Schema.INT64_SCHEMA, expectedValueEST, fieldNameMap);
        testRecordConvert(fieldNameUTC, fieldValueUTCint, Schema.INT32_SCHEMA, expectedValueUTC, fieldNameMap);
        testRecordConvert(fieldNameEST, fieldValueESTint, Schema.INT32_SCHEMA, expectedValueEST, fieldNameMap);
    }

    @Test
    public void testSchemaConvertNumberInSecondsToTimestamp() {
        final String fieldName = "CreatedOn";


        testSchemaConvert(fieldName, Schema.INT64_SCHEMA,
                new FieldNameConverters.NumberInSecondsToTimestampUTC(), LegacySQLTypeName.TIMESTAMP);

        testSchemaConvert(fieldName, Schema.INT64_SCHEMA,
                new FieldNameConverters.NumberInSecondsToTimestampEST(), LegacySQLTypeName.TIMESTAMP);

        testSchemaConvert(fieldName, Schema.INT32_SCHEMA,
                new FieldNameConverters.NumberInSecondsToTimestampUTC(), LegacySQLTypeName.TIMESTAMP);

        testSchemaConvert(fieldName, Schema.INT32_SCHEMA,
                new FieldNameConverters.NumberInSecondsToTimestampEST(), LegacySQLTypeName.TIMESTAMP);

    }

    private void testRecordConvert(String fieldName, Object fieldValue, Schema fieldKafkaType,
                                   String expectedValue, Map<String,String> fieldNameMap) {

        Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
        bigQueryExpectedRecord.put(fieldName, expectedValue);

        Schema kafkaConnectSchema = SchemaBuilder
                .struct()
                .field(fieldName, fieldKafkaType)
                .build();

        Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
        kafkaConnectStruct.put(fieldName, fieldValue);
        SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

        Map<String, Object> bigQueryTestRecord =
                new BigQueryRecordConverter(SHOULD_CONVERT_DOUBLE, fieldNameMap).convertRecord(kafkaConnectRecord);
        assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
    }

    private void testSchemaConvert(String fieldName, Schema kafkaType, FieldNameConverter converter, LegacySQLTypeName bqType) {

        final Map<String,String> fieldNameMap = new HashMap<String, String>() {{
            put(fieldName, converter.converterName());
        }};

        com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
                com.google.cloud.bigquery.Schema.of(
                        com.google.cloud.bigquery.Field.newBuilder(
                                fieldName,
                                bqType
                        ).setMode(
                                com.google.cloud.bigquery.Field.Mode.REQUIRED
                        ).build()
                );

        Schema kafkaConnectTestSchema = SchemaBuilder
                .struct()
                .field(fieldName, kafkaType)
                .build();

        com.google.cloud.bigquery.Schema bigQueryTestSchema =
                new BigQuerySchemaConverter(false, fieldNameMap).convertSchema(kafkaConnectTestSchema);
        assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
    }

    private static SinkRecord spoofSinkRecord(Schema valueSchema, Object value) {
        return new SinkRecord(null, 0, null, null, valueSchema, value, 0);
    }
}
