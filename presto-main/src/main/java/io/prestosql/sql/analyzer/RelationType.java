/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.tree.QualifiedName;

import javax.annotation.concurrent.Immutable;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * TODO: this needs to be merged with RowType at some point (when the type system is unified)
 */
@Immutable
public class RelationType
{
    private List<Field> visibleFields;

    private final List<Field> allFields;

    private final Map<Field, Integer> fieldIndexes;

    public RelationType(Field... fields)
    {
        this(ImmutableList.copyOf(fields));
    }

    public RelationType(List<Field> fields)
    {
        requireNonNull(fields, "fields is null");
        this.allFields = ImmutableList.copyOf(fields);
        this.visibleFields = fields.stream()
                .filter(field -> !field.isHidden())
                .collect(toImmutableList());

        int index = 0;
        ImmutableMap.Builder<Field, Integer> builder = ImmutableMap.builder();
        for (Field field : fields) {
            builder.put(field, index++);
        }
        fieldIndexes = builder.build();
    }

    /**
     * Gets the index of the specified field.
     *
     * @throws IllegalArgumentException when field is not found
     */
    public int indexOf(Field field)
    {
        requireNonNull(field, "field cannot be null");
        Integer index = fieldIndexes.get(field);
        checkArgument(index != null, "Field '%s' not found", field);
        return index;
    }

    /**
     * Gets the field at the specified index.
     */
    public Field getFieldByIndex(int fieldIndex)
    {
        checkElementIndex(fieldIndex, allFields.size(), "fieldIndex");
        return allFields.get(fieldIndex);
    }

    /**
     * Gets only the visible fields.
     * No assumptions should be made about the order of the fields returned from this method.
     * To obtain the index of a field, call indexOf.
     */
    public Collection<Field> getVisibleFields()
    {
        return visibleFields;
    }

    public void setVisibleFields(List<Field> visibleFields) {
        this.visibleFields = visibleFields;
    }

    public int getVisibleFieldCount()
    {
        return visibleFields.size();
    }

    /**
     * Gets all fields including hidden fields.
     * No assumptions should be made about the order of the fields returned from this method.
     * To obtain the index of a field, call indexOf.
     */
    public Collection<Field> getAllFields()
    {
        return ImmutableSet.copyOf(allFields);
    }

    /**
     * Gets the count of all fields including hidden fields.
     */
    public int getAllFieldCount()
    {
        return allFields.size();
    }

    /**
     * Gets all visible fields whose relation alias matches given prefix.
     */
    public List<Field> resolveVisibleFieldsWithRelationPrefix(Optional<QualifiedName> prefix)
    {
        return visibleFields.stream()
                .filter(input -> input.matchesPrefix(prefix))
                .collect(toImmutableList());
    }

    /**
     * Gets the index of all columns matching the specified name
     */
    public List<Field> resolveFields(QualifiedName name)
    {
        return allFields.stream()
                .filter(input -> input.canResolve(name))
                .collect(toImmutableList());
    }

    public boolean canResolve(QualifiedName name)
    {
        return !resolveFields(name).isEmpty();
    }

    /**
     * Creates a new tuple descriptor containing all fields from this tuple descriptor
     * and all fields from the specified tuple descriptor.
     */
    public RelationType joinWith(RelationType other)
    {
        List<Field> fields = ImmutableList.<Field>builder()
                .addAll(this.allFields)
                .addAll(other.allFields)
                .build();

        return new RelationType(fields);
    }

    /**
     * Creates a new tuple descriptor with the relation, and, optionally, the columns aliased.
     */
    public RelationType withAlias(String relationAlias, List<String> columnAliases)
    {
        if (columnAliases != null) {
            checkArgument(columnAliases.size() == visibleFields.size(),
                    "Column alias list has %s entries but '%s' has %s columns available",
                    columnAliases.size(),
                    relationAlias,
                    visibleFields.size());
        }

        int aliasIndex = 0;
        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
        for (Field field : allFields) {
            Optional<String> columnAlias = field.getName();
            if (columnAliases == null) {
                fieldsBuilder.add(Field.newQualified(
                        QualifiedName.of(relationAlias),
                        columnAlias,
                        field.getType(),
                        field.isHidden(),
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        field.isAliased()));
            }
            else if (!field.isHidden()) {
                // hidden fields are not exposed when there are column aliases
                columnAlias = Optional.of(columnAliases.get(aliasIndex));
                aliasIndex++;
                fieldsBuilder.add(Field.newQualified(
                        QualifiedName.of(relationAlias),
                        columnAlias,
                        field.getType(),
                        false,
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        field.isAliased()));
            }
        }

        return new RelationType(fieldsBuilder.build());
    }

    /**
     * Creates a new tuple descriptor with the relation, and, optionally, the columns aliased.
     */
    public RelationType withLateralViewAlias(String baseRelationAlias,
                                             List<String> columnAliases, boolean withOrder)
    {
        if (columnAliases == null) {
            throw new RuntimeException("unnest(lateral view) relation have not any columns!");
        }
        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
        boolean isStruct = false;

        for (Field field: allFields) {
            if (field.getName().isPresent()) {
                isStruct = true;
                break;
            }
        }
        String relationAlias = columnAliases.get(0);
        if (!isStruct && !(columnAliases.size() == 2 && allFields.size() >= 2 && withOrder)) {
            return withAlias(baseRelationAlias, columnAliases);
        }
        if (columnAliases.size() == 1) {
            for (int i = 0; i < allFields.size(); i++) {
                Field field = allFields.get(i);
                Optional<String> columnAlias = field.getName();
                if (!field.isHidden()) {
                    // hidden fields are not exposed when there are column aliases
                    fieldsBuilder.add(Field.newQualified(
                            QualifiedName.of(relationAlias),
                            columnAlias,
                            field.getType(),
                            false,
                            field.getOriginTable(),
                            field.getOriginColumnName(),
                            field.isAliased()));
                }
            }
        } else if (columnAliases.size() == 2 && allFields.size() >= 2 && withOrder) {
            for (int i = 0; i < allFields.size() - 1; i++) {
                Field field = allFields.get(i);
                Optional<String> columnAlias = field.getName();
                String tableAlias = columnAliases.get(columnAliases.size() - 1);
                if (!isStruct) {
                    columnAlias = Optional.of(columnAliases.get(1));
                    tableAlias = baseRelationAlias;
                }
                if (!field.isHidden()) {
                    // hidden fields are not exposed when there are column aliases
                    fieldsBuilder.add(Field.newQualified(
                            QualifiedName.of(tableAlias),
                            columnAlias,
                            field.getType(),
                            false,
                            field.getOriginTable(),
                            field.getOriginColumnName(),
                            field.isAliased()));
                }
            }
            Field last = allFields.get(allFields.size() - 1);
            Optional<String> columnAlias = Optional.of(columnAliases.get(0));
            if (!last.isHidden()) {
                // hidden fields are not exposed when there are column aliases
                fieldsBuilder.add(Field.newQualified(
                        QualifiedName.of(baseRelationAlias),
                        columnAlias,
                        last.getType(),
                        false,
                        last.getOriginTable(),
                        last.getOriginColumnName(),
                        last.isAliased()));
            }
        } else {
            throw new RuntimeException("unproper number of columns!");
        }

        return new RelationType(fieldsBuilder.build());
    }


    /**
     * Creates a new tuple descriptor containing only the visible fields.
     */
    public RelationType withOnlyVisibleFields()
    {
        return new RelationType(visibleFields);
    }

    @Override
    public String toString()
    {
        return allFields.toString();
    }
}
