package sg.bigo.ranger;

/**
 * @author tangyun@bigo.sg
 * @date 10/16/19 5:22 PM
 */
public enum  PrestoAccessType {
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
