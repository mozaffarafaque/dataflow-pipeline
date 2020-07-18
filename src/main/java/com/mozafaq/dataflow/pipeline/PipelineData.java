package com.mozafaq.dataflow.pipeline;

/**
 * @author Mozaffar Afaque
 */
public interface PipelineData<T> {
    <O> PipelineData<O> addTransformer(String name, final Transformer<T, O> transformer);
    void sink(String name, final PipelineSink<T> sink);
}
