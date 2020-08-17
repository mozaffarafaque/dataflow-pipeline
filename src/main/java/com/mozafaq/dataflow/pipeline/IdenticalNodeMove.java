package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdenticalNodeMove<I, O> implements NodeMoveAware<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(IdenticalNodeMove.class);

    final private Transformer<I, O> transformer;
    final private PipelineChain<O> chain;

    public IdenticalNodeMove(Transformer<I, O> transformer, PipelineChain<O> chain) {
        this.transformer = transformer;
        this.chain = chain;
    }

    @Override
    public void init() {
        // Do Nothing
    }

    @Override
    public void onBegin() {
        transformer.onBegin(chain);
    }

    @Override
    public void transfer(I input) {
        transformer.transform(chain, input);
    }

    @Override
    public void onComplete() {
        transformer.onComplete(chain);
    }

    @Override
    public PipelineChain<O> chain() {
        return chain;
    }

    @Override
    public void finish() {
        // Do nothing
    }
}
