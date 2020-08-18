package com.mozafaq.dataflow.pipeline;

/**
 * This is the interface that needs to be implemented
 * to perform specified transformation. The transformation
 * can very well consumes multiple events and give only one
 * event as output.
 *
 * Also, one an have multiple transformer in the data-flow pipeline.
 *
 * @author Mozaffar Afaque
 *
 * @param <I> Input Type
 * @param <O> Transformed output type
 */
public interface Transformer<I, O> {

    /**
     * This should be called in the beginning of the event inject operations.
     * One can choose to implement this. However, if a transformer implements this
     * then onBegin execution must call {@code chain.onBegin} exactly once.
     *
     * @param chain @see PipelineChain
     */
    default void onBegin(PipelineChain<O> chain) {
        chain.onBegin();
    }

    /**
     * This is the actual transformation logic one needs to implement.
     * Once transformation of input object is done then output must be se
     * given as output as {@code chain.output} with output data as
     * argument.
     *
     * @param chain {@see PipelineChain}
     * @param input Input to be processed.
     */
    void transform(PipelineChain<O> chain, I input);

    /**
     * Similar to {@code onBegin} except this is should be called after
     * all the events are processed.
     *
     * @param chain {@see PipelineChain}
     */
    default void onComplete(PipelineChain<O> chain) {
        chain.onComplete();
    }
}
