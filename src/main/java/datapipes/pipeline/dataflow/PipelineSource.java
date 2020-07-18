package datapipes.pipeline.dataflow;

/**
 * @author Mozaffar Afaque
 */
public interface PipelineSource<T> {
    void source(PipelineChain<T> chain);
}
