package com.mozafaq.dataflow.pipeline;

import java.util.List;

/**
 *
 * @author Mozaffar Afaque
 *
 */
public interface PipelineChain<T> {
    String getName();
    void onBegin();
    void output(T out);
    void onComplete();
    List<NodeMoveAware> nodeMoves();
}
