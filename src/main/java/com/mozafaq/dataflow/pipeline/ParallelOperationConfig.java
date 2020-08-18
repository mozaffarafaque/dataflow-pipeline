package com.mozafaq.dataflow.pipeline;

import java.time.Duration;
import java.util.Objects;

/**
 *
 * @author Mozaffar Afaque
 */
public class ParallelOperationConfig {

    private int queueBufferSize;
    private int eventBatchSize;
    private Duration queuePollDuration;
    private int countForInsertAttempt;

    private ParallelOperationConfig() {
    }

    public static ParallelOperationConfigBuilder newBuilder() {
        return new ParallelOperationConfigBuilder();
    }
    public int getQueueBufferSize() {
        return queueBufferSize;
    }

    public Duration getQueuePollDuration() {
        return queuePollDuration;
    }

    public int getCountForInsertAttempt() {
        return countForInsertAttempt;
    }

    public int getEventBatchSize() {
        return eventBatchSize;
    }


    public static class ParallelOperationConfigBuilder {
        private ParallelOperationConfig  parallelOperationConfig = new ParallelOperationConfig();

        private ParallelOperationConfigBuilder() {

        }
        public ParallelOperationConfigBuilder setQueueBufferSize(int queueBufferSize) {
            parallelOperationConfig.queueBufferSize = queueBufferSize;
            return this;
        }

        public ParallelOperationConfigBuilder setEventBatchSize(int eventBatchSize) {
            parallelOperationConfig.eventBatchSize = eventBatchSize;
            return this;
        }

        public ParallelOperationConfigBuilder setQueuePollDuration(Duration queuePollDuration) {
            parallelOperationConfig.queuePollDuration = queuePollDuration;
            return this;
        }

        public ParallelOperationConfigBuilder setCountForInsertAttempt(int countForInsertAttempt) {
            parallelOperationConfig.countForInsertAttempt = countForInsertAttempt;
            return this;
        }

        public ParallelOperationConfig build() {
            Objects.requireNonNull(parallelOperationConfig.queuePollDuration, "Poll duration cannot be null.");
            if (parallelOperationConfig.queuePollDuration.toMillis()  < 1l ||
                    parallelOperationConfig.queueBufferSize < 1 ||
                    parallelOperationConfig.eventBatchSize < 1 ||
                    parallelOperationConfig.countForInsertAttempt < 1) {
                throw  new IllegalArgumentException("Queue size, batch size, poll duration, attemp count must be more than 0!");
            }
            return parallelOperationConfig;
        }
    }
}
