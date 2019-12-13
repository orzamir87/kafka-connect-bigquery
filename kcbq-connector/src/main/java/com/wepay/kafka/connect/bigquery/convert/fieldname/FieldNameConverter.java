package com.wepay.kafka.connect.bigquery.convert.fieldname;


import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.kafka.connect.data.Schema;


public abstract class FieldNameConverter  {

    private final LegacySQLTypeName bqSchemaType;

    /**
     * Create a new FieldNameConverter.
     *
     * @param bqSchemaType The corresponding BigQuery Schema type of the field.
     */
    public FieldNameConverter(LegacySQLTypeName bqSchemaType) {
        this.bqSchemaType = bqSchemaType;
    }

    public LegacySQLTypeName getBQSchemaType() {
        return bqSchemaType;
    }

    public abstract String converterName();

    /**
     * Convert the given KafkaConnect Record Object to a BigQuery Record Object.
     *
     * @param kafkaConnectObject the kafkaConnectObject
     * @return the converted Object
     */
    public abstract Object convert(Object kafkaConnectObject, Schema.Type kafkaConnectSchemaType);

}
