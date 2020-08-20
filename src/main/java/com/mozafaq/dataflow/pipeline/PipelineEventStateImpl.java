package com.mozafaq.dataflow.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author Mozaffar Afaque
 */
class PipelineEventStateImpl<S> implements PipelineEventState<S> {

    // Tree information
    final private PipelineEventStateImpl parentState;
    final private List<PipelineEventStateImpl> childPipelineStates;

    // Transformer used for reaching to this state.
    final private Transformer transformer;

    // Metadata associated with this state
    final private String name;
    final private ParallelOperationConfig parallelOperationConfig;

    // Pipeline chains tree information
    private PipelineChainImpl parentChain;
    private PipelineChainImpl currentChain;
    private List<PipelineChainImpl> childPipelineChains;

    private PipelineEventStateImpl(String name,
                                   Transformer transformer,
                                   PipelineEventStateImpl parentState,
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

    static <T> PipelineEventStateImpl<T> fromSource(String name, final PipelineSource<T> source) {
        Transformer<?, T> sourceTransformer = new SourceTransformer(source);
        return new PipelineEventStateImpl(name, sourceTransformer, null, null);
    }

    @Override
    public <T> PipelineEventState<T> addTransformer(String name, Transformer<S, T> transformer) {
        return createPipelineState(name, transformer, null);
    }

    @Override
    public <T> PipelineEventState<T> addParallelTransformer(String name,
                                                            Transformer<S, T> transformer,
                                                            ParallelOperationConfig parallelOperationConfig) {

        Objects.requireNonNull(parallelOperationConfig,
                "Parallel operation configuration cannot be null!");
        return createPipelineState(name, transformer, parallelOperationConfig);
    }

    private <T> PipelineEventState<T> createPipelineState(String name,
                                                          Transformer<S, T> transformer,
                                                          ParallelOperationConfig parallelOperationConfig) {
        Objects.requireNonNull(transformer);
        PipelineEventStateImpl<T> newState =
                new PipelineEventStateImpl(name, transformer,this, parallelOperationConfig);
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

    List<PipelineEventStateImpl> getChildPipelineStates() {
        return childPipelineStates;
    }

    String getName() {
        return name;
    }

    PipelineEventStateImpl getParentState() {
        return parentState;
    }

    PipelineChainImpl getParentChain() {
        return parentChain;
    }

    void setParentChain(PipelineChainImpl parentChain) {
        this.parentChain = parentChain;
    }

    PipelineChainImpl getCurrentChain() {
        return currentChain;
    }

    void setCurrentChain(PipelineChainImpl currentChain) {
        this.currentChain = currentChain;
    }
}