package com.mozafaq.dataflow.pipeline;

/**
 * This is an event event state in dataflow pipeline. This can be considered as an
 * edge in dataflow pipeline tree.
 * <br><br/>
 * If you got a state, then by adding new transformer you get new state that will be
 * transformed event depending on the transformer that is added.
 * <br><br/>
 * This allows to add transformer that runs concurrently that runs in
 * separate thread then the parent from where the events are received.
 *
 *  @param <T> Type of state.
 *
 * @author Mozaffar Afaque
 */
public interface PipelineState<T> {

    /**
     * Adds the transformer in the pipeline flow.
     *
     * @param name  Name of the transformer. This is just for user to provider
     *             ability to identify node and giving some name.
     *
     * @param transformer that will be applied on the current state. This transfer can
     *                    provide the different type as output.
     *
     * @param <O> Type of the output of transformer.
     *
     * @return New state.
     */
    <O> PipelineState<O> addTransformer(String name,
                                        Transformer<T, O> transformer);

    /**
     *
     * This is exactly similar to other {@code addTransformer} with 2 arguments
     * except changed behaviour that this processing will take place in parallel.
     * Additional parameter defines the configuration for parallelism.
     *
     * @param parallelOperationConfig Mandatory parameter for transformer
     *                               to run in the parallel.
     *
     */
    <O> PipelineState<O> addParallelTransformer(String name,
                                                Transformer<T, O> transformer,
                                                ParallelOperationConfig parallelOperationConfig);

    /**
     * Adds the sink in the dataflow pipeline. This is like a leaf node in
     * in the data-flow graph.
     *
     * @param name Name of the sink.
     *
     * @param sink Sink object, implementation of {@code PipelineSink}
     */
    void sink(String name, final PipelineSink<T> sink);
}
