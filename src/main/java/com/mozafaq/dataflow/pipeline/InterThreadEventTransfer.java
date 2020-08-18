package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Mozaffar Afaque
 */
class InterThreadEventTransfer<I, O> implements EventTransfer<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(InterThreadEventTransfer.class);

    final private ParallelOperationConfig parallelOperationConfig;
    final private Transformer<I, O> transformer;
    final private PipelineChainImpl<O> chain;
    private ParallelOperationController parallelOperationController;

    public InterThreadEventTransfer(Transformer<I, O> transformer, PipelineChainImpl<O> chain, ParallelOperationConfig parallelOperationConfig) {
        this.transformer = transformer;
        this.chain = chain;
        this.parallelOperationConfig = parallelOperationConfig;
    }

    @Override
    public synchronized void init() {
        if (parallelOperationController != null) {
            LOG.warn("Already initialized, cannot be initialize more than once.");
            return;
        }
        parallelOperationController =
                new ParallelOperationController(transformer, chain, parallelOperationConfig);

        parallelOperationController.init();
    }

    @Override
    public void onBegin() {
        parallelOperationController.onBegin(true);
    }

    @Override
    public void transfer(I input) {
        parallelOperationController.transfer(input);
    }

    @Override
    public void onComplete() {
        parallelOperationController.onComplete();
    }

    @Override
    public PipelineChainImpl<O> chain() {
        return chain;
    }

    @Override
    public void finish() {
        parallelOperationController.finish();

    }
}
