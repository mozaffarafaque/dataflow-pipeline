package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * @author Mozaffar Afaque
 */
public class PipelineChainImpl<T> implements PipelineChain<T> {

    private List<NodeMoveAware> chains;
    private String name;

    public static final PipelineChainImpl PIPELINE_CHAIN_SINK = new PipelineChainImpl("<Sink>", Collections.emptyList()) {
        @Override
        public void output(Object out) {}
        @Override
        public void onComplete() {}
        @Override
        public void onBegin() { }
    };

    PipelineChainImpl(String name,  List<NodeMoveAware> chains) {
        this.name = name;
        this.chains = chains;
    }

    @Override
    public void output(T out) {
        for (NodeMoveAware chain : chains) {
            chain.transfer(out);
        }
    }

    @Override
    public void onComplete() {
        for (NodeMoveAware chain : chains) {
            chain.onComplete();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void onBegin() {
        for (NodeMoveAware chain : chains) {
            chain.onBegin();
        }
    }

    @Override
    public List<NodeMoveAware> nodeMoves() {
        return chains;
    }
};
