package app;

import app.enums.OffsetType;
import com.google.common.base.Objects;

import java.util.Map;

public class Config {
    public String appName;
    public String master;
    public String directory;
    public String defaultKeyspace;
    public OffsetType offsetOverride;
    public String insertStatement;
    public Map<String, String> sparkConfigs;
    public Map<String, KafkaInfo> kafkaAppConfigs;
    public Map<String, TableInfo> cassandraTables;

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appName", appName)
                .add("master", master)
                .add("directory", directory)
                .add("defaultKeyspace", defaultKeyspace)
                .add("offsetOverride", offsetOverride)
                .add("insertStatement", insertStatement)
                .add("sparkConfigs", sparkConfigs)
                .add("kafkaAppConfigs", kafkaAppConfigs)
                .add("cassandraTables", cassandraTables)
                .toString();
    }
}
