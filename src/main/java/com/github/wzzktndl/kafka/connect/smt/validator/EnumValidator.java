package com.github.wzzktndl.kafka.connect.smt.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class EnumValidator implements ConfigDef.Validator {

    private final Class<? extends Enum> enumClass;

    public EnumValidator(Class<? extends Enum> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value instanceof String) {
            try {
                Enum.valueOf(enumClass, (String) value);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(name, value, "Value must be one of: " + String.join(", ", getEnumNames(enumClass)));
            }
        } else {
            throw new ConfigException(name, value, "Value must be a string.");
        }

    }

    public static <E extends Enum<E>> String[] getEnumNames(Class<E> enumClass) {
        return java.util.Arrays.stream(enumClass.getEnumConstants())
                .map(Enum::name)
                .toArray(String[]::new);
    }
}
