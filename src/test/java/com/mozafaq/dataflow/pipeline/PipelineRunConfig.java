package com.mozafaq.dataflow.pipeline;

import java.util.List;

/**
 * @author Mozaffar Afaque
 */
public class PipelineRunConfig<T> {

    private boolean beginCall;
    private boolean completeCall;
    private DelayConfig delayConfig = new DelayConfig(1,1,1,1);
    private List<T> events;

    public PipelineRunConfig(boolean beginCall, boolean completeCall, DelayConfig delayConfig, List<T> events) {
        this.beginCall = beginCall;
        this.completeCall = completeCall;
        this.events = events;
        if (delayConfig != null) {
            this.delayConfig = delayConfig;
        }
    }

    public boolean isBeginCall() {
        return beginCall;
    }

    public boolean isCompleteCall() {
        return completeCall;
    }

    public List<T> getEvents() {
        return events;
    }

    public DelayConfig getDelayConfig() {
        return delayConfig;
    }

    @Override
    public String toString() {
        return "PipelineRunConfig{" +
                beginCall +
                ", " + completeCall +
                ", Delay:" + delayConfig +
                ", events=" + events +
                '}';
    }
}
