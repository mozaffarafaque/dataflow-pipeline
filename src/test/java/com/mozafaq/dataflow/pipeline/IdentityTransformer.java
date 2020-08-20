package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

abstract class TestIdentityTransformer<I,O> implements Transformer<I, O> {
    private  final Logger LOG = LoggerFactory.getLogger(TestIdentityTransformer.class);

    final private long delayBeforeOutputMillis;
    final private long delayAfterOutputMillis;
    private Set<? extends Object> exceptionEvents = null;

    private boolean exceptionOnBegin;
    private boolean exceptionOnComplete;

    public TestIdentityTransformer(long delayBeforeOutputMillis, long delayAfterOutputMillis) {
        this.delayBeforeOutputMillis = Math.max(0l, delayBeforeOutputMillis);
        this.delayAfterOutputMillis = Math.max(0l, delayAfterOutputMillis);
    }

    public void setExceptionOnBegin(boolean exceptionOnBegin) {
        this.exceptionOnBegin = exceptionOnBegin;
    }

    public void setExceptionOnComplete(boolean exceptionOnComplete) {
        this.exceptionOnComplete = exceptionOnComplete;
    }

    @Override
    public void onBegin(PipelineChain<O> chain) {
        if (exceptionOnBegin) {
            throw new IllegalArgumentException("Exception on begin");
        }
        chain.onBegin();
    }

    @Override
    public void onComplete(PipelineChain<O> chain) {
        if (exceptionOnComplete) {
            throw new IllegalArgumentException("Exception on complete");
        }
        chain.onComplete();
    }

    @Override
    public void transform(PipelineChain<O> chain, I input) {
        try {
            if(delayBeforeOutputMillis > 0) {
                Thread.sleep(delayBeforeOutputMillis);
            }
            LOG.info("Output from identity transformer - " + input);
            if (exceptionEvents != null && exceptionEvents.contains(input)) {
                throw new NullPointerException(input + " is exception events. Events: " + exceptionEvents);
            }
            transferData(chain, input);
            if(delayAfterOutputMillis > 0) {
                Thread.sleep(delayAfterOutputMillis);
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public abstract void transferData(PipelineChain<O> chain, I input);

    public void setExceptionEvents(Set<? extends Object> exceptionEvents) {
        this.exceptionEvents = exceptionEvents;
    }

    @Override
    public String toString() {
        return "TestIdentityTransformer{" +
                "delayBeforeOutputMillis=" + delayBeforeOutputMillis +
                ", delayAfterOutputMillis=" + delayAfterOutputMillis +
                ", exceptionEvents=" + exceptionEvents +
                '}';
    }
}

class IntToIntIdentity extends TestIdentityTransformer<Integer, Integer> {

    public IntToIntIdentity(long delayBeforeOutputMillis, long delayAfterOutputMillis) {
        super(delayBeforeOutputMillis, delayAfterOutputMillis);
    }
    public IntToIntIdentity() {
        super(1,1);
    }

    @Override
    public void transferData(PipelineChain<Integer> chain, Integer input) {
        chain.output(input);
    }
}

class IntToStrIdentity extends TestIdentityTransformer<Integer, String> {

    public IntToStrIdentity(long delayBeforeOutputMillis, long delayAfterOutputMillis) {
        super(delayBeforeOutputMillis, delayAfterOutputMillis);
    }

    public IntToStrIdentity() {
        super(1,1);
    }

    @Override
    public void transferData(PipelineChain<String> chain, Integer input) {
        chain.output(String.valueOf(input));
    }
}

class StrToIntIdentity extends TestIdentityTransformer<String, Integer> {

    public StrToIntIdentity(long delayBeforeOutputMillis, long delayAfterOutputMillis) {
        super(delayBeforeOutputMillis, delayAfterOutputMillis);
    }

    public StrToIntIdentity() {
        super(1, 1);
    }

    @Override
    public void transferData(PipelineChain<Integer> chain, String input) {
        chain.output(Integer.parseInt(input));
    }
}

class StrToStrIdentity extends TestIdentityTransformer<String, String> {

    public StrToStrIdentity(long delayBeforeOutputMillis, long delayAfterOutputMillis) {
        super(delayBeforeOutputMillis, delayAfterOutputMillis);
    }

    public StrToStrIdentity() {
        super(1, 1);
    }

    @Override
    public void transferData(PipelineChain<String> chain, String input) {
        chain.output(input);
    }
}

