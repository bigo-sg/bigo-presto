package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DefaultColumnNamer {
    static final String DEFAULT_COLUMN_NAME_PREFIX = "_col";
    static final String UNIQUE_COLUMN_NAME_PREFIX = "__";

    static ImmutableList.Builder<Field> assignDefaultNameIfNeeded(List<Field> fields) {
        // add default column name if needed.
        List<Field> outputFieldsWithDefaultName = new ArrayList<>();
        int defaultColumnNameIndex = 0;

        if (fields != null) {
            for(Field field : fields) {
                Optional<String> fieldName = field.getName();

                if (!fieldName.isPresent()) {
                    String defaultFieldName = DEFAULT_COLUMN_NAME_PREFIX + defaultColumnNameIndex;
                    defaultColumnNameIndex++;

                    outputFieldsWithDefaultName.add(Field.newUnqualified(defaultFieldName, field.getType()));
                } else {
                    outputFieldsWithDefaultName.add(field);
                }
            }
        }

        // make sure each column name is unique.
        // Note: we only keeps the lowercase name in nameSet to avoid semantic analysis error
        Set<String> nameSet = new HashSet<>();
        int uniqueColumnNameIndex = 1;
        List<Field> outputFieldsWithUniqueName = new ArrayList<>();

        for(Field field : outputFieldsWithDefaultName) {
            if (!nameSet.add(field.getName().get().toLowerCase())) {
                String uniqueFieldName = field.getName().get() + UNIQUE_COLUMN_NAME_PREFIX + uniqueColumnNameIndex;
                uniqueColumnNameIndex++;
                nameSet.add(uniqueFieldName.toLowerCase());

                outputFieldsWithUniqueName.add(Field.newUnqualified(uniqueFieldName, field.getType()));
            } else {
                outputFieldsWithUniqueName.add(field);
            }
        }

        ImmutableList.Builder<Field> ret = ImmutableList.builder();
        ret.addAll(outputFieldsWithUniqueName);

        return ret;
    }
}