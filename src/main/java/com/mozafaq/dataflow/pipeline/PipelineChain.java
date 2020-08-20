package com.mozafaq.dataflow.pipeline;

import java.util.List;

/**
 *
 * This help in the passing over the data from one node to another in the
 * pipeline graph. Any transformer implementation will get this as
 * argument in all 3 APIs (@code onBegin, @code transform, @code onComplete
 * - First and last are optional to implement). These chain provide handles
 * for making call to subsequent transformer by invoking the respective APIs
 *
 *
 * @author Mozaffar Afaque
 *
 */
public interface PipelineChain<T> {
    String getName();
    void onBegin();
    void output(T out);
    void onComplete();
}
