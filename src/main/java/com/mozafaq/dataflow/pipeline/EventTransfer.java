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
    PipelineChain<O> chain();
    void finish();
}
