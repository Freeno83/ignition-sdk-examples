package com.inductiveautomation.ignition.examples.kafka.datasink;

import java.time.LocalDateTime;

/**
 * Written By: Nick Robinson
 * Date: 05-Oct-2021
 * Content: Definition and tracking of sink statistics
 */

public class MessageStats {

    private LocalDateTime lastMessageTime;
    private long successCount, failedCount;
    private String name;

    public MessageStats(String source){
        this.name = source;
        this.successCount = 0;
        this.failedCount = 0;
    }

    public long getSuccessCount() {
        return successCount;
    }

    public long getFailedCount() {
        return failedCount;
    }

    public void addOneFailedCount() {
        failedCount++;
    }

    public void addOneSuccessCount() {
        successCount++;
    }

    public void setLastMessageTime() {
        this.lastMessageTime = LocalDateTime.now();
    }

    public String getLastMessageTime() {
        return String.valueOf(lastMessageTime);
    }

    public String getSourceName() {
        return this.name;
    }

    public void reset() {
        this.successCount = 0;
        this.failedCount = 0;
        this.lastMessageTime = null;
    }
}
