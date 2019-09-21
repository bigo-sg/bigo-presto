package org.apache.ranger.authorization.presto.authorizer.utils;

/**
 * @author tangyun@bigo.sg
 * @date 9/26/19 4:21 PM
 */
public enum PrestoAccessType {
    CREATE("create"),
    DROP("drop"),
    SELECT("select"),
    DELETE("update"),
    SHOW("select"),
    INSERT("update"),
    ALTER("alter");
    private String name;
    PrestoAccessType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}