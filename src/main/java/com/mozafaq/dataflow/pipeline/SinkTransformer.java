package com.mozafaq.dataflow.pipeline;

/**
 * @author Mozaffar Afaque
 */
class SinkTransformer implements Transformer
{
    private final PipelineSink pipelineSink;

    public SinkTransformer(PipelineSink pipelineSink) {
        this.pipelineSink = pipelineSink;
    }

    @Override
    public void onBegin(PipelineChain chain) {
        pipelineSink.onBegin();
    }

    @Override
    public void onComplete(PipelineChain chain) {
        pipelineSink.onComplete();
    }

    @Override
    public void transform(PipelineChain chain, Object input) {
        pipelineSink.sink(input);
    }
}