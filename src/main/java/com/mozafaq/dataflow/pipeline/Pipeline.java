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
            throw new IllegalStateException("Source already set!");
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
                            getNodeMoves(pipelineNode.getTransformer(),
                                    Collections.singletonList(PipelineChainImpl.PIPELINE_CHAIN_SINK),
                                    pipelineNode.getParallelOperationConfig())
                    );
            pipelineNode.setPipelineChains(Collections.singletonList(chain));
            return;
        }

        List<PipelineDataImpl> childNodes = pipelineNode.getChildPipelines();
        for (PipelineDataImpl node : childNodes) {
            buildChainRecursive(node);
        }

        List<PipelineChainImpl> chains = childNodes.stream()
                .map(node -> new PipelineChainImpl(
                        node.getName(),
                        getNodeMoves(node.getTransformer(), node.getPipelineChains(), node.getParallelOperationConfig())
                    ))
                .collect(Collectors.toUnmodifiableList());

        pipelineNode.setPipelineChains(chains);
    }

    private List<EventTransfer> getNodeMoves(Transformer transformer, List<PipelineChainImpl> chains, ParallelOperationConfig parallelOperationConfig) {
        return chains.stream()
                .map(chain -> getNodeMoveAware(transformer, chain, parallelOperationConfig))
                .collect(Collectors.toUnmodifiableList());
    }

    private EventTransfer getNodeMoveAware(Transformer transformer, PipelineChainImpl chain, ParallelOperationConfig parallelOperationConfig) {

        if (parallelOperationConfig == null) {
            return new IntraThreadEventTransfer(transformer, chain);
        } else {
            return new InterThreadEventTransfer(transformer, chain, parallelOperationConfig);
        }
    }

    private void initNodeMoves(PipelineChain pipelineChain, final boolean isStart) {
        if (pipelineChain.getEventTransfers().isEmpty()) {
            // Sink Node
            return;
        }
        List<EventTransfer> childNodeMoves = pipelineChain.getEventTransfers();

        for (EventTransfer move : childNodeMoves) {
            if (isStart) {
                move.init();
            } else {
                move.finish();
            }
        }
        for (EventTransfer move : childNodeMoves) {
            initNodeMoves(move.chain(), isStart);
        }

    }

    public synchronized void build() {
        Objects.requireNonNull(root);

        if (isFrozen) {
            throw new IllegalStateException("It is already built. " +
                    "Cannot perform build operation again.");
        }
        buildChainRecursive(root);
        isFrozen = true;
    }

    public void run() {

        if (!isFrozen) {
            throw new IllegalArgumentException("You cannot run without pipeline build!");
        }

        List<PipelineChainImpl> chains = root.getPipelineChains();

        for (PipelineChainImpl chain: chains) {
            initNodeMoves(chain, true);
        }

        Transformer transformer = root.getTransformer();
        for (PipelineChainImpl chain: chains) {
            transformer.transform(chain, null);
        }

        for (PipelineChainImpl chain: chains) {
            initNodeMoves(chain, false);
        }
    }
}