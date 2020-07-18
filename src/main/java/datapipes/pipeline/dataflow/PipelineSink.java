package datapipes.pipeline.dataflow;

/**
 * @author Mozaffar Afaque
 */
public interface PipelineSink<T> {
    default void onBegin() {
        // Do nothing
    }
    void sink(T object);
    default void onComplete() {
        // Do Nothing
    } ;
}
