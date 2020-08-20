package com.mozafaq.dataflow.pipeline;


/**
 *
 * @author Mozaffar Afaque
 */
interface EventTransfer<I, O> {

    void init();
    void onBegin();
    void transfer(I input);
    void onComplete();
    PipelineChainImpl<O> chain();
    void finish();
    void killRunningParallelExecutions();
}
