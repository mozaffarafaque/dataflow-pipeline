package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Mozaffar Afaque
 */
class IntraThreadEventTransfer<I, O> implements EventTransfer<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(IntraThreadEventTransfer.class);

    final private Transformer<I, O> transformer;
    final private PipelineChain<O> chain;

    public IntraThreadEventTransfer(Transformer<I, O> transformer, PipelineChain<O> chain) {
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
