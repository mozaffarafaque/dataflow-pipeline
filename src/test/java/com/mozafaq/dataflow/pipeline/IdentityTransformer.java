package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class TestIdentityTransformer<I,O> implements Transformer<I, O> {
    private static final Logger LOG = LoggerFactory.getLogger(IdentityTransformer.class);

    final private long delayBeforeOutputMillis;
    final private long delayAfterOutputMillis;

    public TestIdentityTransformer(long delayBeforeOutputMillis, long delayAfterOutputMillis) {
        this.delayBeforeOutputMillis = Math.max(1l, delayBeforeOutputMillis);
        this.delayAfterOutputMillis = Math.max(1l, delayAfterOutputMillis);
    }
    @Override
    public void transform(PipelineChain<O> chain, I input) {
        try {
            Thread.sleep(delayBeforeOutputMillis);
            LOG.info("Output from identity transformer - " + input);
            transferData(chain, input);
            Thread.sleep(delayAfterOutputMillis);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public abstract void transferData(PipelineChain<O> chain, I input);
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

