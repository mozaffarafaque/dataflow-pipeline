package datapipes.pipeline.dataflow;

/**
 * @author Mozaffar Afaque
 *
 * @param <I> Input Type
 * @param <O> Transformed output type
 */
public interface Transformer<I, O> {

    default void onBegin(PipelineChain<O> chain) {
        chain.onBegin();
    }

    void transform(PipelineChain<O> chain, I input);

    default void onComplete(PipelineChain<O> chain) {
        chain.onComplete();
    }
}
