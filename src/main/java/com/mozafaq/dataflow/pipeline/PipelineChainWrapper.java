package com.mozafaq.dataflow.pipeline;

import java.util.List;

public class PipelineChainWrapper<T> implements PipelineChain<T>  {

    final private PipelineChain<T> originalPipelineChain;
    final private PipelineChainWrapper<T> nextPipelineChainWrapper;

    public PipelineChainWrapper(PipelineChain<T> originalPipelineChain, PipelineChainWrapper<T> nextPipelineChainWrapper) {
        this.originalPipelineChain = originalPipelineChain;
        this.nextPipelineChainWrapper = nextPipelineChainWrapper;
    }

    @Override
    public String getName() {
        return originalPipelineChain.getName();
    }

    @Override
    public void onBegin() {
        originalPipelineChain.onBegin();
    }

    @Override
    public void output(T out) {
        originalPipelineChain.output(out);
    }

    @Override
    public void onComplete() {
        originalPipelineChain.onComplete();
    }

    @Override
    public List<NodeMoveAware> nodeMoves() {
        return originalPipelineChain.nodeMoves();
    }

    public void forceCompletion() {
        nextPipelineChainWrapper.forceCompletion();
    }
}
