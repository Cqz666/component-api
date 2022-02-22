package com.cqz.component.flink.api.interval_join.datastream;

import lombok.Data;

@Data
public class UserClickLog {
    private String userID;
    private String eventTime;
    private String eventType;
    private String pageID;
}
