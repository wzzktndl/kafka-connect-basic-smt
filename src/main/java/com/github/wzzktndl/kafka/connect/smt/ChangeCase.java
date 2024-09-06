package com.github.wzzktndl.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import com.github.wzzktndl.kafka.connect.smt.validator.EnumValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.*;

public abstract class ChangeCase<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger log = LoggerFactory.getLogger(ChangeCase.class);

    public static final String OVERVIEW_DOC =
            "Change text case on top of each field, e.g. word to WORD";

    public interface ConfigName {
        String FIELD_LIST_CONFIG = "fields.list";
        String CASE = "case";
    }

    public enum Case {
        Uppercase, Lowercase
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_LIST_CONFIG,
                    ConfigDef.Type.LIST,
                    new ArrayList<String>(),
                    ConfigDef.Importance.MEDIUM,
                    "List of fields to change case.")
            .define(ConfigName.CASE,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new EnumValidator(Case.class),
                    ConfigDef.Importance.HIGH,
                    "Uppercase or Lowercase."
            );
    private static final String PURPOSE = "change case";

    private List<String> fields;
    private Case toCase;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> prop) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, prop);
        fields = config.getList(ConfigName.FIELD_LIST_CONFIG);
        toCase = Case.valueOf(config.getString(ConfigName.CASE));
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else
            return applyWithSchema(record);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    private R applySchemaless(R record) {
        return record;
    }

    private R applyWithSchema(R record) {
        final Struct recordValue = requireStruct(operatingValue(record), PURPOSE);
        Struct updatedValue = new Struct(operatingSchema(record));

        for (Field field : recordValue.schema().fields()) {
            recursiveTransform(field, recordValue, updatedValue);
        }

        return newRecord(record, operatingSchema(record), updatedValue);
    }

    private void recursiveTransform(Field field, Struct recordValue, Struct updatedValue) {
        if (field.schema().type() == Schema.Type.STRUCT) {
            Struct recordValueInStruct = (Struct) recordValue.get(field);
            Schema schemaInStruct = field.schema();
            Struct updatedValueInStruct = new Struct(schemaInStruct);

            for (Field fieldInStruct : field.schema().fields()) {
                recursiveTransform(fieldInStruct, recordValueInStruct, updatedValueInStruct);
            }
            updatedValue.put(field, updatedValueInStruct);
        } else {
            if (fields.contains(field.name()) && field.schema().type().equals(Schema.Type.STRING)) {
                switch (toCase) {
                    case Uppercase:
                        updatedValue.put(field, recordValue.get(field).toString().toUpperCase());
                        break;
                    case Lowercase:
                        updatedValue.put(field, recordValue.get(field).toString().toLowerCase());
                        break;
                }
            } else
                updatedValue.put(field.name(), recordValue.get(field));
        }
    }

    protected abstract Schema operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final class Key<R extends ConnectRecord<R>> extends ChangeCase<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    updatedSchema,
                    updatedValue,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends ChangeCase<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp());
        }
    }
}
