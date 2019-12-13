package com.wepay.kafka.connect.bigquery.convert.fieldname;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import org.apache.kafka.connect.data.Schema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

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

        private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
        public static final String CONVERTER_NAME = "NumberInSecondsToTimestampUTC";

        public NumberInSecondsToTimestampUTC() {
            super(utcTimeZone, CONVERTER_NAME);
        }
    }

    public static class NumberInSecondsToTimestampEST extends NumberInSecondsToTimestamp {

        private static final TimeZone estTimeZone = TimeZone.getTimeZone("EST");
        public static final String CONVERTER_NAME = "NumberInSecondsToTimestampEST";

        public NumberInSecondsToTimestampEST() {
            super(estTimeZone, CONVERTER_NAME);
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
            } catch (ParseException e) {
                throw new ConversionConnectException("can't convert " + kafkaConnectObject + " to yyyyMMdd Date");
            }
        }
    }

    /**
     * Class for converting Kafka integer or long to timestamp in big query
     */
    protected static class NumberInSecondsToTimestamp extends FieldNameConverter {

        private final TimeZone timeZone;
        private final String converterName;

        public NumberInSecondsToTimestamp(TimeZone timeZone, String converterName) {
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
            switch (kafkaConnectSchemaType)
            {
                case INT64:
                    return getBqTimestampFormat(timeZone).format(new java.util.Date((Long) kafkaConnectObject * 1000));
                case INT32:
                    return getBqTimestampFormat(timeZone).format(new java.util.Date(((Integer)kafkaConnectObject).longValue() * 1000));
                default:
                    throw new ConversionConnectException("can't convert " + kafkaConnectSchemaType + " to Timestamp");
            }
        }
    }

    protected static SimpleDateFormat getBqTimestampFormat(TimeZone timeZone) {
        SimpleDateFormat bqTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        bqTimestampFormat.setTimeZone(timeZone);
        return bqTimestampFormat;
    }

    protected static SimpleDateFormat getBQDateFormat() {
        return new SimpleDateFormat("yyyy-MM-dd");
    }
}
