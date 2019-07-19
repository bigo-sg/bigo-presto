package io.prestosql.sql.parser.debug;

import java.lang.reflect.Field;

public class FieldUtils {

    public static String filedsToString(Object object) {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("class name:" + object.getClass().getName() + "\n");
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            Object value;
            try {
                value = field.get(object);
                stringBuilder.append(field.getName() + ":" + value + "\n");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return stringBuilder.toString();
    }
}
