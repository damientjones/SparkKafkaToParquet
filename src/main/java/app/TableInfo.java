package app;

import com.google.common.base.Objects;

public class TableInfo {
    public String table;
    public String keyspace;
    public String fields;

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("keyspace", keyspace)
                .add("fields", fields)
                .toString();
    }
}