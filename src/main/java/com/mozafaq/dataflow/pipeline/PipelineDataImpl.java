package com.mozafaq.dataflow.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author Mozaffar Afaque
 */
public class PipelineDataImpl<S> implements PipelineData<S> {

    private List<PipelineDataImpl> childPipelines;
    final private Transformer transformer;
    private PipelineDataImpl parent;
    private List<PipelineChainImpl> pipelineChains;
    private String name;

    private PipelineDataImpl(String name, Transformer transformer, PipelineDataImpl parent) {
        this.transformer = transformer;
        this.name = name;
        this.childPipelines = new ArrayList<>();
        this.parent = parent;
    }

    List<PipelineChainImpl> getPipelineChains() {
        return pipelineChains;
    }

    void setPipelineChains(List<PipelineChainImpl> pipelineChains) {
        this.pipelineChains = pipelineChains;
    }

    public static <T> PipelineDataImpl<T> fromSource(String name, final PipelineSource<T> source) {

        PipelineDataImpl<T> pipelineNodeImpl =
                new PipelineDataImpl(name, new SourceTransformer(source), null);
        return pipelineNodeImpl;
    }

    public <T> PipelineData<T> addTransformer(String name, final Transformer<S, T> transformer) {
        Objects.requireNonNull(transformer);

        PipelineDataImpl<T> newData = new PipelineDataImpl(name, transformer, this);
        this.childPipelines.add(newData);
        return newData;
    }

    public void sink(String name, final PipelineSink<S> sink) {
        Objects.requireNonNull(sink);
        PipelineDataImpl<S> newData =
                new PipelineDataImpl(name, new SinkTransformer(sink), this);
        childPipelines.add(newData);
    }

    Transformer getTransformer() {
        return transformer;
    }

    List<PipelineDataImpl> getChildPipelines() {
        return childPipelines;
    }

    String getName() {
        return name;
    }
}

