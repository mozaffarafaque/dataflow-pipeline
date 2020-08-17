package com.mozafaq.dataflow.pipeline;

public interface NodeMoveAware<I, O> {

    void init();
    void onBegin();
    void transfer(I input);
    void onComplete();
    PipelineChain<O> chain();
    void finish();
}
