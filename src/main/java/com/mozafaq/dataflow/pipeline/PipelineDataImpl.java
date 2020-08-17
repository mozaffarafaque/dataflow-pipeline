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
    private ConcurrentTransformerConfig concurrentTransformerConfig;
    private String name;

    private PipelineDataImpl(String name, Transformer transformer, PipelineDataImpl parent, ConcurrentTransformerConfig concurrentTransformerConfig) {
        this.transformer = transformer;
        this.name = name;
        this.childPipelines = new ArrayList<>();
        this.parent = parent;
        this.concurrentTransformerConfig = concurrentTransformerConfig;
    }

    List<PipelineChainImpl> getPipelineChains() {
        return pipelineChains;
    }

    void setPipelineChains(List<PipelineChainImpl> pipelineChains) {
        this.pipelineChains = pipelineChains;
    }

    public static <T> PipelineDataImpl<T> fromSource(String name, final PipelineSource<T> source) {

        PipelineDataImpl<T> pipelineNodeImpl =
                new PipelineDataImpl(name, new SourceTransformer(source), null, null);
        return pipelineNodeImpl;
    }

    @Override
    public <T> PipelineData<T> addTransformer(String name, final Transformer<S, T> transformer) {
        return addTransformer(name, transformer, null);
    }

    @Override
    public <T> PipelineData<T> addTransformer(String name,
                                              final Transformer<S, T> transformer,
                                              ConcurrentTransformerConfig concurrentTransformerConfig) {
        Objects.requireNonNull(transformer);

        PipelineDataImpl<T> newData = new PipelineDataImpl(name, transformer,this, concurrentTransformerConfig);
        this.childPipelines.add(newData);
        return newData;
    }

    @Override
    public void sink(String name, final PipelineSink<S> sink) {
        Objects.requireNonNull(sink);
        PipelineDataImpl<S> newData =
                new PipelineDataImpl(name, new SinkTransformer(sink), this,null);
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

    public ConcurrentTransformerConfig getConcurrentTransformerConfig() {
        return concurrentTransformerConfig;
    }
}

