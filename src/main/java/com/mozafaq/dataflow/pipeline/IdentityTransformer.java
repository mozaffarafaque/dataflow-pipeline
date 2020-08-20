package com.mozafaq.dataflow.pipeline;

/**
 *
 * @author Mozaffar Afaque
 */
class IdentityTransformer<T> implements Transformer<T, T> {

    private static final Transformer IDENTITY = new IdentityTransformer();

    public static <E> Transformer<E, E> identity() {
        return IDENTITY;
    }

    @Override
    public void onBegin(PipelineChain<T> chain) {
        chain.onBegin();
    }

    @Override
    public void transform(PipelineChain<T> chain, T input) {
        chain.output(input);
    }

    @Override
    public void onComplete(PipelineChain<T> chain) {
        chain.onComplete();
    }
}
