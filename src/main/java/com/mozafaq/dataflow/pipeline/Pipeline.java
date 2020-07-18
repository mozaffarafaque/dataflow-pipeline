package com.mozafaq.dataflow.pipeline;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Mozaffar Afaque
 */
public class Pipeline {

    private PipelineDataImpl root;
    boolean isFrozen = false;
    private Pipeline() {
    }

    public static Pipeline create() {
        return new Pipeline();
    }

    public <T> PipelineData<T> fromSource(String name, PipelineSource<T> source) {
        Objects.requireNonNull(source);
        if (root != null) {
            throw new IllegalStateException("Source already set");
        }
        root = PipelineDataImpl.fromSource(name, source);
        return root;
    }

    private void buildChainRecursive(PipelineDataImpl pipelineNode) {
        if (pipelineNode.getChildPipelines().isEmpty()) {
            // Sink Node
            PipelineChainImpl chain =
                    new PipelineChainImpl(
                            pipelineNode.getName(),
                            pipelineNode.getTransformer(),
                            Collections.singletonList(PipelineChainImpl.PIPELINE_CHAIN));
            pipelineNode.setPipelineChains(Collections.singletonList(chain));
            return;
        }

        List<PipelineDataImpl> childNodes = pipelineNode.getChildPipelines();
        for (PipelineDataImpl node : childNodes) {
            buildChainRecursive(node);
        }

        List<PipelineChainImpl> chains = childNodes.stream()
                .map(
                        node -> new PipelineChainImpl(
                                node.getName(),
                                node.getTransformer(),
                                node.getPipelineChains()
                        ))
                .collect(Collectors.toUnmodifiableList());

        pipelineNode.setPipelineChains(chains);

    }

    public synchronized void build() {
        Objects.requireNonNull(root);

        if (isFrozen ) {
            throw new IllegalStateException("It is already built. Cannot be done again.");
        }
        buildChainRecursive(root);
        isFrozen = true;
    }

    public void run() {
        if (!isFrozen) {
            throw new IllegalArgumentException("You cannot run without pipeline build!");
        }

        Transformer transformer = root.getTransformer();
        List<PipelineChainImpl> chains = root.getPipelineChains();
        for (PipelineChainImpl chain: chains) {
            transformer.transform(chain, null);
        }
    }
}
