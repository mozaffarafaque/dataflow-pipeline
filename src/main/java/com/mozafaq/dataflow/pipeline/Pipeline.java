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


    enum Action {
        START,
        FINISH,
        KILL
    }

    private PipelineEventStateImpl rootState;
    private PipelineChainImpl rootChain;
    private boolean isFrozen = false;
    private Pipeline() {
    }

    public static Pipeline create() {
        return new Pipeline();
    }

    public synchronized  <T> PipelineEventState<T> fromSource(String name, PipelineSource<T> source) {
        Objects.requireNonNull(source);
        if (rootState != null) {
            throw new IllegalStateException("Source already set!");
        }
        rootState = PipelineEventStateImpl.fromSource(name, source);
        Transformer<T, T> identity = Transformer.identity();
        PipelineEventState<T> eventState = rootState.addTransformer("<SourceIdentity-" + name + ">", identity);
        return eventState;
    }

    private void buildChainRecursive(PipelineEventStateImpl pipelineState) {
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

        List<PipelineEventStateImpl> childStates = pipelineState.getChildPipelineStates();
        for (PipelineEventStateImpl childState : childStates) {
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

    private void initEventTransfers(PipelineChainImpl pipelineChain, final Action action) {

        List<EventTransfer> eventTransfers = pipelineChain.getEventTransfers();

        if (eventTransfers.isEmpty()) {
            // Sink Node
            return;
        }

        for (EventTransfer transfer : eventTransfers) {
            if (action == Action.START) {
                transfer.init();
            } else if (action == Action.FINISH) {
                transfer.finish();
            } else if (action == Action.KILL) {
                transfer.killRunningParallelExecutions();
            }
        }

        for (EventTransfer transfer : eventTransfers) {
            initEventTransfers(transfer.chain(), action);
        }
    }

    public synchronized void build() {
        Objects.requireNonNull(rootState);

        if (isFrozen) {
            throw new IllegalStateException("It is already built. " +
                    "Cannot perform build operation again.");
        }

        buildChainRecursive(rootState);

        List<PipelineChainImpl> childChains = rootState.getChildPipelineChains();
        assert  childChains.size() == 1;
        rootChain = childChains.get(0);
        isFrozen = true;
    }

    public synchronized void run() {

        if (!isFrozen) {
            throw new IllegalArgumentException("You cannot run without pipeline build!");
        }

        try {
            initEventTransfers(rootChain, Action.START);
            Transformer transformer = rootState.getTransformer();
            transformer.transform(rootChain, null);
            initEventTransfers(rootChain, Action.FINISH);
        } catch (RuntimeException e) {
            initEventTransfers(rootChain, Action.KILL);
            throw new RuntimeException("There was an error occurred while executing the pipeline", e);
        }
    }
}