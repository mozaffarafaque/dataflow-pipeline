package com.mozafaq.dataflow.pipeline;

/**
 * @author Mozaffar Afaque
 */
public interface PipelineSource<T> {
    void source(PipelineChain<T> chain);
}
