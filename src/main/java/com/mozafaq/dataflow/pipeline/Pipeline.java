package com.mozafaq.dataflow.pipeline;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 * The main handler for the user to create the data-flow pipeline, build
 * and execute it.
 *
 * @author Mozaffar Afaque
 */
public class Pipeline {

    private PipelineStateImpl rootState;
    private PipelineChainImpl rootChain;
    private boolean isFrozen = false;
    private Pipeline() {
    }

    public static Pipeline create() {
        return new Pipeline();
    }

    public synchronized  <T> PipelineState<T>  fromSource(String name, PipelineSource<T> source) {
        Objects.requireNonNull(source);
        if (rootState != null) {
            throw new IllegalStateException("Source already set!");
        }
        rootState = PipelineStateImpl.fromSource(name, source);
        return rootState;
    }

    private void buildChainRecursive(PipelineStateImpl pipelineState) {
        if (pipelineState.getChildPipelineStates().isEmpty()) {
            // Sink Node

            List<EventTransfer> eventTransfers = createEventTransfers(
                    pipelineState.getTransformer(),
                    Collections.singletonList(PipelineChainImpl.PIPELINE_CHAIN_SINK),
                    pipelineState.getParallelOperationConfig());

            PipelineChainImpl chain = new PipelineChainImpl(pipelineState.getName(), eventTransfers);
            pipelineState.setChildPipelineChains(Collections.singletonList(chain));
            return;
        }

        List<PipelineStateImpl> childStates = pipelineState.getChildPipelineStates();
        for (PipelineStateImpl childState : childStates) {
            buildChainRecursive(childState);
        }

        List<PipelineChainImpl> childChains = childStates.stream()
                .map(state -> new PipelineChainImpl(
                        state.getName(),
                        createEventTransfers(
                                state.getTransformer(),
                                state.getChildPipelineChains(),
                                state.getParallelOperationConfig())
                    ))
                .collect(Collectors.toUnmodifiableList());

        pipelineState.setChildPipelineChains(childChains);
    }

    private List<EventTransfer> createEventTransfers(Transformer transformer,
                                                     List<PipelineChainImpl> chains,
                                                     ParallelOperationConfig parallelOperationConfig) {
        return chains.stream()
                .map(chain -> createEventTransfer(transformer, chain, parallelOperationConfig))
                .collect(Collectors.toUnmodifiableList());
    }

    private EventTransfer createEventTransfer(Transformer transformer,
                                              PipelineChainImpl chain,
                                              ParallelOperationConfig parallelOperationConfig) {

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
        Objects.requireNonNull(rootState);

        if (isFrozen) {
            throw new IllegalStateException("It is already built. " +
                    "Cannot perform build operation again.");
        }
        buildChainRecursive(rootState);
        isFrozen = true;
    }

    public void run() {

        if (!isFrozen) {
            throw new IllegalArgumentException("You cannot run without pipeline build!");
        }

        List<PipelineChainImpl> chains = rootState.getChildPipelineChains();

        for (PipelineChainImpl chain: chains) {
            initEventTransfers(chain, true);
        }

        Transformer transformer = rootState.getTransformer();
        for (PipelineChainImpl chain : chains) {
            transformer.transform(chain, null);
        }

        for (PipelineChainImpl chain: chains) {
            initEventTransfers(chain, false);
        }
    }
}