package com.mozafaq.dataflow.pipeline;

public class ConcurrentTransformerConfig {

    private int queueBufferSize;
    private int eventBatchSize;
    private long queueFetchPollTimeMillis;
    private int countForInsertAttempt = 10;

    public ConcurrentTransformerConfig(int queueBufferSize, int eventBatchSize, long queueFetchPollTimeMillis, int countForInsertAttempt) {
        this.queueBufferSize = queueBufferSize;
        this.eventBatchSize = eventBatchSize;
        this.queueFetchPollTimeMillis = queueFetchPollTimeMillis;
        this.countForInsertAttempt = countForInsertAttempt;
    }

    public int getQueueBufferSize() {
        return queueBufferSize;
    }

    public long getQueueFetchPollTimeMillis() {
        return queueFetchPollTimeMillis;
    }

    public int getCountForInsertAttempt() {
        return countForInsertAttempt;
    }

    public int getEventBatchSize() {
        return eventBatchSize;
    }
}
