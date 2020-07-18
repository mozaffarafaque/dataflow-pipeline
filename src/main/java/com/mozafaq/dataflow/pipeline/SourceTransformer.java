package com.mozafaq.dataflow.pipeline;

/**
 * @author Mozaffar Afaque
 */
class SourceTransformer implements Transformer
{
    private final PipelineSource pipelineSource;

    public SourceTransformer(PipelineSource pipelineSource) {
        this.pipelineSource = pipelineSource;
    }

    @Override
    public void onBegin(PipelineChain chain) {
       //At source, don't call on begin, This should be client choice
    }


    @Override
    public void transform(PipelineChain chain, Object input) {
        pipelineSource.source(chain);
    }

    @Override
    public void onComplete(PipelineChain chain) {
        //At complete, don't call on complete, This should be client choice
    }
}