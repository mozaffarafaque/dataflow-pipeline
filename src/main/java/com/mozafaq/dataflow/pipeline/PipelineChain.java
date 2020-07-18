package com.mozafaq.dataflow.pipeline;

/**
 *
 * @author Mozaffar Afaque
 *
 */
public interface PipelineChain<T> {
    String getName();
    void onBegin();
    void output(T out);
    void onComplete();
}
