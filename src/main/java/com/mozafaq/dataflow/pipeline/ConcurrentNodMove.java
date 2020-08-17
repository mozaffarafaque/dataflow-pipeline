package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentNodMove<I, O> implements NodeMoveAware<I, O>{

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentNodMove.class);

    final private ConcurrentTransformerConfig concurrentTransformerConfig;
    final private Transformer<I, O> transformer;
    final private PipelineChain<O> chain;
    private ConcurrentTransformationSource concurrentTransformationSource;

    public ConcurrentNodMove(Transformer<I, O> transformer, PipelineChain<O> chain, ConcurrentTransformerConfig concurrentTransformerConfig) {
        this.transformer = transformer;
        this.chain = chain;
        this.concurrentTransformerConfig = concurrentTransformerConfig;
    }

    @Override
    public synchronized void init() {
        if (concurrentTransformationSource != null) {
            LOG.warn("Already initialized, cannot be initialize more than once.");
            return;
        }
        concurrentTransformationSource =
                new ConcurrentTransformationSource(transformer, chain, concurrentTransformerConfig);

        concurrentTransformationSource.init();
    }

    @Override
    public void onBegin() {
        concurrentTransformationSource.onBegin(true);
    }

    @Override
    public void transfer(I input) {
        concurrentTransformationSource.transfer(input);
    }

    @Override
    public void onComplete() {
        concurrentTransformationSource.onComplete();
    }

    @Override
    public PipelineChain<O> chain() {
        return chain;
    }

    @Override
    public void finish() {
        concurrentTransformationSource.finish(false);

    }
}
