package com.mozafaq.dataflow.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author Mozaffar Afaque
 */
class PipelineStateImpl<S> implements PipelineState<S> {

    final private List<PipelineStateImpl> childPipelineStates;
    final private Transformer transformer;
    final private PipelineStateImpl parent;
    final private ParallelOperationConfig parallelOperationConfig;
    final private String name;

    private List<PipelineChainImpl> pipelineChains;

    private PipelineStateImpl(String name,
                              Transformer transformer,
                              PipelineStateImpl parent,
                              ParallelOperationConfig parallelOperationConfig) {
        this.transformer = transformer;
        this.name = name;
        this.childPipelineStates = new ArrayList<>();
        this.parent = parent;
        this.parallelOperationConfig = parallelOperationConfig;
    }

    List<PipelineChainImpl> getPipelineChains() {
        return pipelineChains;
    }

    void setPipelineChains(List<PipelineChainImpl> pipelineChains) {
        this.pipelineChains = pipelineChains;
    }

    static <T> PipelineStateImpl<T> fromSource(String name, final PipelineSource<T> source) {

        PipelineStateImpl<T> pipelineNodeImpl =
                new PipelineStateImpl(name, new SourceTransformer(source), null, null);
        return pipelineNodeImpl;
    }

    @Override
    public <T> PipelineState<T> addTransformer(String name,
                                               Transformer<S, T> transformer) {
        return createPipelineState(name, transformer, null);
    }

    @Override
    public <T> PipelineState<T> addParallelTransformer(String name,
                                                       Transformer<S, T> transformer,
                                                       ParallelOperationConfig parallelOperationConfig) {

        Objects.requireNonNull(parallelOperationConfig, "Parallel Operation Configuration must be non-null");
        return createPipelineState(name, transformer, parallelOperationConfig);
    }

    private <T> PipelineState<T> createPipelineState(String name,
                                                    Transformer<S, T> transformer,
                                                    ParallelOperationConfig parallelOperationConfig) {
        Objects.requireNonNull(transformer);
        PipelineStateImpl<T> newState = new PipelineStateImpl(name, transformer,this, parallelOperationConfig);
        this.childPipelineStates.add(newState);
        return newState;
    }

    @Override
    public void sink(String name, final PipelineSink<S> sink) {
        Objects.requireNonNull(sink);
        PipelineStateImpl<S> newData =
                new PipelineStateImpl(name, new SinkTransformer(sink), this,null);
        childPipelineStates.add(newData);
    }

    ParallelOperationConfig getParallelOperationConfig() {
        return parallelOperationConfig;
    }

    Transformer getTransformer() {
        return transformer;
    }

    List<PipelineStateImpl> getChildPipelineStates() {
        return childPipelineStates;
    }

    String getName() {
        return name;
    }
}