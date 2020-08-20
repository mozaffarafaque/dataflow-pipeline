package com.mozafaq.dataflow.pipeline;

import java.util.List;

public class PipelineRunConfig<T> {

    private boolean isBeginEnabled;
    private boolean isCompleteEnabled;
    private long beforeRecordOutWait;
    private long afterRecordOutWait;
    private List<T> inputEvents ;

    public PipelineRunConfig(boolean isBeginEnabled, boolean isCompleteEnabled, long beforeRecordOutWait, long afterRecordOutWait, List<T> inputEvents) {
        this.isBeginEnabled = isBeginEnabled;
        this.isCompleteEnabled = isCompleteEnabled;
        this.beforeRecordOutWait = beforeRecordOutWait;
        this.afterRecordOutWait = afterRecordOutWait;
        this.inputEvents = inputEvents;
    }

    public boolean isBeginEnabled() {
        return isBeginEnabled;
    }

    public boolean isCompleteEnabled() {
        return isCompleteEnabled;
    }

    public long getBeforeRecordOutWait() {
        return beforeRecordOutWait;
    }

    public long getAfterRecordOutWait() {
        return afterRecordOutWait;
    }

    public List<T> getInputEvents() {
        return inputEvents;
    }

    @Override
    public String toString() {
        return "PipelineRunConfig{" +
                 isBeginEnabled +
                ", " + isCompleteEnabled +
                ", " + beforeRecordOutWait +
                ", " + afterRecordOutWait +
                ", events=" + inputEvents +
                '}';
    }
}
