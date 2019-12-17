package com.wepay.kafka.connect.bigquery.convert.fieldname;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import org.apache.kafka.connect.data.Schema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class FieldNameConverters {

    static {
        FieldNameConverterRegistry.registerConverterType(NumberInSecondsToTimestampUTC.CONVERTER_NAME,
                new NumberInSecondsToTimestampUTC());
        FieldNameConverterRegistry.registerConverterType(NumberInSecondsToTimestampEST.CONVERTER_NAME,
                new NumberInSecondsToTimestampEST());
        FieldNameConverterRegistry.registerConverterType(IntyyyyMMddToDate.CONVERTER_NAME,
                new IntyyyyMMddToDate());
    }

    public static class NumberInSecondsToTimestampUTC extends NumberInSecondsToTimestamp {

        public static final String CONVERTER_NAME = "NumberInSecondsToTimestampUTC";

        public NumberInSecondsToTimestampUTC() {
            super(ZoneId.of("UTC"), CONVERTER_NAME);
        }
    }

    public static class NumberInSecondsToTimestampEST extends NumberInSecondsToTimestamp {

        public static final String CONVERTER_NAME = "NumberInSecondsToTimestampEST";

        public NumberInSecondsToTimestampEST() {
            super(ZoneId.of("America/New_York"), CONVERTER_NAME);
        }
    }

    /**
     * Class for converting Kafka integer to date in big query
     */
    public static class IntyyyyMMddToDate extends FieldNameConverter {

        private static final SimpleDateFormat intDateFormat = new SimpleDateFormat("yyyyMMdd");
        public static final String CONVERTER_NAME = "IntyyyyMMddToDate";


        public IntyyyyMMddToDate() {
            super(LegacySQLTypeName.DATE);
        }

        @Override
        public String converterName() {
            return this.CONVERTER_NAME;
        }

        @Override
        public String convert(Object kafkaConnectObject, Schema.Type kafkaConnectSchemaType) {
            String dateNumberStr;
            switch (kafkaConnectSchemaType)
            {
                case INT64:
                    dateNumberStr = ((Long)kafkaConnectObject).toString();
                    break;
                case INT32:
                    dateNumberStr = ((Integer)kafkaConnectObject).toString();
                    break;
                default:
                    throw new ConversionConnectException("can't convert " + kafkaConnectSchemaType + " to Timestamp");
            }

            try {
                return getBQDateFormat().format(intDateFormat.parse(dateNumberStr));
            } catch (Exception e) {
                throw new ConversionConnectException("can't convert " + kafkaConnectObject + " to yyyyMMdd Date");
            }
        }
    }

    /**
     * Class for converting Kafka integer or long to timestamp in big query
     */
    protected static class NumberInSecondsToTimestamp extends FieldNameConverter {

        private final ZoneId timeZone;
        private final String converterName;

        public NumberInSecondsToTimestamp(ZoneId timeZone, String converterName) {
            super(LegacySQLTypeName.TIMESTAMP);
            this.timeZone = timeZone;
            this.converterName = converterName;
        }

        @Override
        public String converterName() {
            return this.converterName;
        }

        @Override
        public String convert(Object kafkaConnectObject, Schema.Type kafkaConnectSchemaType) {
            LocalDateTime ldt;
            switch (kafkaConnectSchemaType)
            {
                case INT64:
                    ldt = LocalDateTime.ofEpochSecond((Long) kafkaConnectObject, 0, ZoneOffset.UTC);
                    break;
                case INT32:
                    ldt = LocalDateTime.ofEpochSecond(((Integer)kafkaConnectObject).longValue(), 0, ZoneOffset.UTC);
                    break;
                default:
                    throw new ConversionConnectException("can't convert " + kafkaConnectSchemaType + " to Timestamp");
            }
            return getBqTimestampFormat.format(ldt.atZone(timeZone));
        }
    }

    private static final DateTimeFormatter getBqTimestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXXX");

    protected static SimpleDateFormat getBQDateFormat() {
        return new SimpleDateFormat("yyyy-MM-dd");
    }
}
