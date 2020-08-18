package com.mozafaq.dataflow.pipeline;

import java.util.Collections;
import java.util.List;

/**
 * @author Mozaffar Afaque
 */
public class PipelineChainImpl<T> implements PipelineChain<T> {

    private List<EventTransfer> eventTransfers;
    private String name;

    public static final PipelineChainImpl PIPELINE_CHAIN_SINK = new PipelineChainImpl("<Sink>", Collections.emptyList()) {
        @Override
        public void output(Object out) {}
        @Override
        public void onComplete() {}
        @Override
        public void onBegin() { }
    };

    PipelineChainImpl(String name, List<EventTransfer> eventTransfers) {
        this.name = name;
        this.eventTransfers = eventTransfers;
    }

    @Override
    public void output(T out) {
        for (EventTransfer transfer : eventTransfers) {
            transfer.transfer(out);
        }
    }

    @Override
    public void onComplete() {
        for (EventTransfer transfer : eventTransfers) {
            transfer.onComplete();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void onBegin() {
        for (EventTransfer chain : eventTransfers) {
            chain.onBegin();
        }
    }

    @Override
    public List<EventTransfer> getEventTransfers() {
        return eventTransfers;
    }
};
