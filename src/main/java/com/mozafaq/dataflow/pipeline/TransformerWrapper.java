package com.mozafaq.dataflow.pipeline;

public class TransformerWrapper<I, O> implements Transformer<I, O> {

    final Transformer<I, O> originalTransformer;
    final TransformerWrapper<I, O> nextTransformerWrapper;

    public TransformerWrapper(Transformer<I, O> originalTransformer, TransformerWrapper<I, O> nextTransformerWrapper) {
        this.originalTransformer = originalTransformer;
        this.nextTransformerWrapper = nextTransformerWrapper;
    }

    @Override
    public void onBegin(PipelineChain<O> chain) {
        originalTransformer.onBegin(chain);
    }

    @Override
    public void transform(PipelineChain<O> chain, I input) {
        originalTransformer.transform(chain, input);
    }

    @Override
    public void onComplete(PipelineChain<O> chain) {
        originalTransformer.onComplete(chain);
    }

    final public void forceCompletion() {
        nextTransformerWrapper.forceCompletion();
    }
}
