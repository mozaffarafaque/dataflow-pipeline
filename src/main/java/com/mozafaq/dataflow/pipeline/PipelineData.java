package com.mozafaq.dataflow.pipeline;

/**
 * @author Mozaffar Afaque
 */
public interface PipelineData<T> {
    <O> PipelineData<O> addTransformer(String name, final Transformer<T, O> transformer);
    <O> PipelineData<O> addTransformer(String name,
                                       final Transformer<T, O> transformer,
                                       ConcurrentTransformerConfig concurrentTransformerConfig);
    void sink(String name, final PipelineSink<T> sink);
}
