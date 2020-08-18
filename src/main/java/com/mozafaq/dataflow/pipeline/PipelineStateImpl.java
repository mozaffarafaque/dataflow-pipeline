package com.mozafaq.dataflow.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author Mozaffar Afaque
 */
class PipelineStateImpl<S> implements PipelineState<S> {

    // Tree information
    final private PipelineStateImpl parentState;
    final private List<PipelineStateImpl> childPipelineStates;

    // Transformer used for reaching to this state.
    final private Transformer transformer;

    // Metadata associated with this state
    final private String name;
    final private ParallelOperationConfig parallelOperationConfig;

    // Pipeline chains tree information
    private PipelineChainImpl parentChain;
    private List<PipelineChainImpl> childPipelineChains;

    private PipelineStateImpl(String name,
                              Transformer transformer,
                              PipelineStateImpl parentState,
                              ParallelOperationConfig parallelOperationConfig) {
        this.transformer = transformer;
        this.name = name;
        this.childPipelineStates = new ArrayList<>();
        this.parentState = parentState;
        this.parallelOperationConfig = parallelOperationConfig;
    }

    List<PipelineChainImpl> getChildPipelineChains() {
        return childPipelineChains;
    }

    void setChildPipelineChains(List<PipelineChainImpl> childPipelineChains) {
        this.childPipelineChains = childPipelineChains;
    }

    static <T> PipelineStateImpl<T> fromSource(String name, final PipelineSource<T> source) {
        Transformer<?, T> sourceTransformer = new SourceTransformer(source);
        return new PipelineStateImpl(name, sourceTransformer, null, null);
    }

    @Override
    public <T> PipelineState<T> addTransformer(String name, Transformer<S, T> transformer) {
        return createPipelineState(name, transformer, null);
    }

    @Override
    public <T> PipelineState<T> addParallelTransformer(String name,
                                                       Transformer<S, T> transformer,
                                                       ParallelOperationConfig parallelOperationConfig) {

        Objects.requireNonNull(parallelOperationConfig,
                "Parallel operation configuration cannot be null!");
        return createPipelineState(name, transformer, parallelOperationConfig);
    }

    private <T> PipelineState<T> createPipelineState(String name,
                                                     Transformer<S, T> transformer,
                                                     ParallelOperationConfig parallelOperationConfig) {
        Objects.requireNonNull(transformer);
        PipelineStateImpl<T> newState =
                new PipelineStateImpl(name, transformer,this, parallelOperationConfig);
        this.childPipelineStates.add(newState);
        return newState;
    }

    @Override
    public void sink(String name, final PipelineSink<S> sink) {
        Objects.requireNonNull(sink);
        Transformer<S, ?> sinkTransformer = new SinkTransformer(sink);
        createPipelineState(name, sinkTransformer, null);
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

    PipelineStateImpl getParentState() {
        return parentState;
    }

    public PipelineChainImpl getParentChain() {
        return parentChain;
    }

    public void setParentChain(PipelineChainImpl parentChain) {
        this.parentChain = parentChain;
    }
}