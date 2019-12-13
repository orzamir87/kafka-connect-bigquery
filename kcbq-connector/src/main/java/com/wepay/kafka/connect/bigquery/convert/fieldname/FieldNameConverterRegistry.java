package com.wepay.kafka.connect.bigquery.convert.fieldname;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FieldNameConverterRegistry {

    private static final Logger logger = LoggerFactory.getLogger(FieldNameConverterRegistry.class);

    private static Map<String, FieldNameConverter> converterMap = new ConcurrentHashMap<>();
    private static Map<String, FieldNameConverter> converterTypesMap = new ConcurrentHashMap<>();

    public static void registerConverterType(String converterType, FieldNameConverter converter) {
        converterTypesMap.put(converterType, converter);
    }

    public static void register(String fieldName, String converterName) {
        if(converterTypesMap.containsKey(converterName)) {
            converterMap.put(fieldName, converterTypesMap.get(converterName));
        }
        else {
            logger.warn("FieldName converter: " + converterName + " does not exists, FieldNameConverter will not apply!");
        }
    }

    public static FieldNameConverter getConverter(String fieldName) {
        return converterMap.get(fieldName);
    }

    public static boolean isRegisteredFieldName(String fieldName) {
        return fieldName != null && converterMap.containsKey(fieldName);
    }
}
