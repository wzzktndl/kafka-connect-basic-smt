package com.github.wzzktndl.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ChangeCaseTest {

    private static final Logger log = LoggerFactory.getLogger(ChangeCaseTest.class);

    private final ChangeCase<SourceRecord> xformKey = new ChangeCase.Key<>();
    private final ChangeCase<SourceRecord> xformValue = new ChangeCase.Value<>();

    @AfterEach void tearDown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testInvalidCase() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(ChangeCase.ConfigName.CASE, "")));
    }

    @Test
    public void testSimpleChangeCase() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ChangeCase.ConfigName.FIELD_LIST_CONFIG, "Firstname,Lastname,Lower");
        props.put(ChangeCase.ConfigName.CASE, "Uppercase");
        xformValue.configure(props);

        Schema childSchema = SchemaBuilder.struct()
                .field("Lower", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Upper", Schema.OPTIONAL_STRING_SCHEMA);
        Schema parentSchema = SchemaBuilder.struct()
                .field("Firstname", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Lastname", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Info", childSchema);

        Struct input = new Struct(parentSchema)
                .put("Firstname", "John")
                .put("Lastname", "Doe")
                .put("Info", new Struct(childSchema)
                        .put("Lower", "this is lower text")
                        .put("Upper", "THIS IS UPPER TEXT"));

        final SourceRecord record = new SourceRecord(null, null, "topic", parentSchema, input);
        final SourceRecord result = xformValue.apply(record);

        assertEquals(input.get("Firstname").toString().toUpperCase(), ((Struct)result.value()).get("Firstname"));
        assertEquals(input.get("Lastname").toString().toUpperCase(), ((Struct)result.value()).get("Lastname"));
        assertEquals(input.getStruct("Info").get("Lower").toString().toUpperCase(), ((Struct)result.value()).getStruct("Info").get("Lower"));
    }

    @Test
    public void checkVersion() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(xformKey.version(), xformValue.version());
    }

}