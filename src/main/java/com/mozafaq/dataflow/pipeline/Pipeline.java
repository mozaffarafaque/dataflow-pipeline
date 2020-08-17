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
                                    pipelineNode.getConcurrentTransformerConfig())
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
                        getNodeMoves(node.getTransformer(), node.getPipelineChains(), node.getConcurrentTransformerConfig())
                    ))
                .collect(Collectors.toUnmodifiableList());

        pipelineNode.setPipelineChains(chains);
    }

    private List<NodeMoveAware> getNodeMoves(Transformer transformer, List<PipelineChainImpl> chains, ConcurrentTransformerConfig concurrentTransformerConfig) {
        return chains.stream()
                .map(chain -> getNodeMoveAware(transformer, chain, concurrentTransformerConfig))
                .collect(Collectors.toUnmodifiableList());
    }

    private NodeMoveAware getNodeMoveAware(Transformer transformer, PipelineChainImpl chain, ConcurrentTransformerConfig concurrentTransformerConfig) {

        if (concurrentTransformerConfig == null) {
            return new IdenticalNodeMove(transformer, chain);
        } else {
            return new ConcurrentNodMove(transformer, chain, concurrentTransformerConfig);
        }
    }

    private void initNodeMoves(PipelineChain pipelineChain, boolean isStart) {
        if (pipelineChain.nodeMoves().isEmpty()) {
            // Sink Node
            return;
        }
        List<NodeMoveAware> childNodeMoves = pipelineChain.nodeMoves();
        for (NodeMoveAware move : childNodeMoves) {
            initNodeMoves(move.chain(), isStart);
        }
        for (NodeMoveAware move : childNodeMoves) {
            if (isStart) {
                move.init();
            } else {
                move.finish();
            }
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