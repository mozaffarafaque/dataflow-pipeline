package com.mozafaq.dataflow.pipeline;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 * The main handler for the user to create the data-flow pipeline, build and execute it.
 *
 * @author Mozaffar Afaque
 */
public class Pipeline {

    private PipelineStateImpl root;
    private boolean isFrozen = false;
    private Pipeline() {
    }

    public static Pipeline create() {
        return new Pipeline();
    }

    public <T> PipelineState<T> fromSource(String name, PipelineSource<T> source) {
        Objects.requireNonNull(source);
        if (root != null) {
            throw new IllegalStateException("Source already set!");
        }
        root = PipelineStateImpl.fromSource(name, source);
        return root;
    }

    private void buildChainRecursive(PipelineStateImpl pipelineNode) {
        if (pipelineNode.getChildPipelineStates().isEmpty()) {
            // Sink Node

            List<EventTransfer> eventTransfers = createEventTransfers(
                    pipelineNode.getTransformer(),
                    Collections.singletonList(PipelineChainImpl.PIPELINE_CHAIN_SINK),
                    pipelineNode.getParallelOperationConfig());

            PipelineChainImpl chain = new PipelineChainImpl(pipelineNode.getName(), eventTransfers);
            pipelineNode.setPipelineChains(Collections.singletonList(chain));
            return;
        }

        List<PipelineStateImpl> childNodes = pipelineNode.getChildPipelineStates();
        for (PipelineStateImpl node : childNodes) {
            buildChainRecursive(node);
        }

        List<PipelineChainImpl> chains = childNodes.stream()
                .map(node -> new PipelineChainImpl(
                        node.getName(),
                        createEventTransfers(node.getTransformer(), node.getPipelineChains(), node.getParallelOperationConfig())
                    ))
                .collect(Collectors.toUnmodifiableList());

        pipelineNode.setPipelineChains(chains);
    }

    private List<EventTransfer> createEventTransfers(Transformer transformer, List<PipelineChainImpl> chains, ParallelOperationConfig parallelOperationConfig) {
        return chains.stream()
                .map(chain -> createEventTransfer(transformer, chain, parallelOperationConfig))
                .collect(Collectors.toUnmodifiableList());
    }

    private EventTransfer createEventTransfer(Transformer transformer, PipelineChainImpl chain, ParallelOperationConfig parallelOperationConfig) {

        if (parallelOperationConfig == null) {
            return new IntraThreadEventTransfer(transformer, chain);
        } else {
            return new InterThreadEventTransfer(transformer, chain, parallelOperationConfig);
        }
    }

    private void initEventTransfers(PipelineChainImpl pipelineChain, final boolean isStart) {

        List<EventTransfer> eventTransfers = pipelineChain.getEventTransfers();

        if (eventTransfers.isEmpty()) {
            // Sink Node
            return;
        }

        for (EventTransfer transfer : eventTransfers) {
            if (isStart) {
                transfer.init();
            } else {
                transfer.finish();
            }
        }

        for (EventTransfer transfer : eventTransfers) {
            initEventTransfers(transfer.chain(), isStart);
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
            initEventTransfers(chain, true);
        }

        Transformer transformer = root.getTransformer();
        for (PipelineChainImpl chain: chains) {
            transformer.transform(chain, null);
        }

        for (PipelineChainImpl chain: chains) {
            initEventTransfers(chain, false);
        }
    }
}