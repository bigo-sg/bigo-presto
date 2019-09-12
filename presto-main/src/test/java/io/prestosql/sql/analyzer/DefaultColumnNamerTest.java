package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.CharType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.*;

public class DefaultColumnNamerTest {

    private List<Field> getDefaultNameIfNeeded(List<Field> fields) {
        ImmutableList.Builder<Field> ret = DefaultColumnNamer.assignDefaultNameIfNeeded(fields);

        return ret.build();
    }

    private Field createField(Optional<String> name) {
        return new Field(Optional.empty(), name, CharType.createCharType(1), false, Optional.empty(), Optional.empty(), false);
    }

    @Test
    public void testWithEmptyFiled() {
        List<Field> fields = new ArrayList<>();

        List<Field> ret = getDefaultNameIfNeeded(fields);

        Assert.assertEquals(ret.size(), 0);
    }

    @Test
    public void testWithNullFiled() {
        List<Field> ret = getDefaultNameIfNeeded(null);

        Assert.assertEquals(ret.size(), 0);
    }

    @Test
    public void testWithNoNameField() {
        List<Field> fields = new ArrayList<>();
        Field f1 = createField(Optional.empty());
        Field f2 = createField(Optional.empty());

        fields.add(f1);
        fields.add(f2);

        List<Field> ret = getDefaultNameIfNeeded(fields);

        Assert.assertEquals(ret.size(), 2);
        Assert.assertEquals(ret.get(0).getName().get(), "_col0");
        Assert.assertEquals(ret.get(1).getName().get(), "_col1");
    }

    @Test
    public void testWithSameNameField() {
        List<Field> fields = new ArrayList<>();
        Field f1 = createField(Optional.of("colName"));
        Field f2 = createField(Optional.of("colName"));

        fields.add(f1);
        fields.add(f2);

        List<Field> ret = getDefaultNameIfNeeded(fields);

        Assert.assertEquals(ret.size(), 2);
        Assert.assertEquals(ret.get(0).getName().get(), "colName");
        Assert.assertEquals(ret.get(1).getName().get(), "colName__1");
    }

    @Test
    public void testWithSameNameFieldMathchDefaultName() {
        List<Field> fields = new ArrayList<>();
        Field f1 = createField(Optional.of("_col0"));
        Field f2 = createField(Optional.empty());
        Field f3 = createField(Optional.of("_col0"));
        Field f4 = createField(Optional.empty());

        fields.add(f1);
        fields.add(f2);
        fields.add(f3);
        fields.add(f4);

        List<Field> ret = getDefaultNameIfNeeded(fields);

        Assert.assertEquals(ret.size(), 4);
        Assert.assertEquals(ret.get(0).getName().get(), "_col0");
        Assert.assertEquals(ret.get(1).getName().get(), "_col0__1");
        Assert.assertEquals(ret.get(2).getName().get(), "_col0__2");
        Assert.assertEquals(ret.get(3).getName().get(), "_col1");
    }
}