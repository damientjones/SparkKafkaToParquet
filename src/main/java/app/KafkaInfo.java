package app;

import com.google.common.base.Objects;

public class KafkaInfo {
    public String brokerList;
    public String topicList;
    public String batchInterval;
    public String batchIntervalUnit;

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("brokerList", brokerList)
                .add("topicList", topicList)
                .add("batchInterval", batchInterval)
                .add("batchIntervalUnit", batchIntervalUnit)
                .toString();
    }
}