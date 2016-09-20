package app;

import java.util.Map;

public class Config {
    private String appName;
    private String master;
    private String directory;
    private String offsetOverride;
    private Map<String, String> sparkConfigs;
    private Map<String, Map<String, String>> kafkaAppConfigs;
    private Map<String, Map<String, String>> cassandraTables;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getOffsetOverride() {
        return offsetOverride;
    }

    public void setOffsetOverride(String offsetOverride) {
        this.offsetOverride = offsetOverride;
    }

    public Map<String, String> getSparkConfigs() {
        return sparkConfigs;
    }

    public void setSparkConfigs(Map<String, String> sparkConfigs) {
        this.sparkConfigs = sparkConfigs;
    }

    public Map<String, Map<String, String>> getKafkaAppConfigs() {
        return kafkaAppConfigs;
    }

    public void setKafkaAppConfigs(Map<String, Map<String, String>> kafkaAppConfigs) {
        this.kafkaAppConfigs = kafkaAppConfigs;
    }

    public Map<String, Map<String, String>> getCassandraTables() {
        return cassandraTables;
    }

    public void setCassandraTables(Map<String, Map<String, String>> cassandraTables) {
        this.cassandraTables = cassandraTables;
    }
}
