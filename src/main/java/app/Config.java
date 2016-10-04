package app;

import app.enums.OffsetType;

import java.util.Map;

public class Config {
    public String appName;
    public String master;
    public String directory;
    public String defaultKeyspace;
    public OffsetType offsetOverride;
    public String insertStatement;
    public Map<String, String> sparkConfigs;
    public Map<String, Map<String, String>> kafkaAppConfigs;
    public Map<String, Map<String, String>> cassandraTables;

}
