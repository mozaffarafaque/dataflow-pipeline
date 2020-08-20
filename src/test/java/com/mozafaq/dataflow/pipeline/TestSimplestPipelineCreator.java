package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author Mozaffar Afaque
 */
class Source implements PipelineSource<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private boolean isBeginCalled;
    private boolean isCompleteCalled;
    private List<Integer> events;

    public Source(boolean isBeginCalled, boolean isCompleteCalled, List<Integer> events) {
        this.isBeginCalled = isBeginCalled;
        this.isCompleteCalled = isCompleteCalled;
        this.events = events;
    }

    @Override
    public void source(PipelineChain<Integer> chain) {

        LOG.info("Starting at source ");
        if (isBeginCalled) {
            chain.onBegin();
        }
        LOG.info("Starting at source begin completed ");
        for (Integer e : events) {
            chain.output(e);
        }

        LOG.info("Starting at source out completed ");

        if (isCompleteCalled) {
            chain.onComplete();
        }
        LOG.info("Starting at source complete completed ");
    }
}

class ChildSquare implements Transformer<Integer, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(ChildSquare.class);

    @Override
    public void transform(PipelineChain<Integer> chain, Integer input) {
        LOG.info("before input proceed -" + chain.getName() + ", Processing " + input);

        chain.output(input*input);
        LOG.info("after out completed - " + chain.getName() + ", Processed input " + input);
    }
}

class ChildCube implements Transformer<Integer, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(ChildCube.class);

    @Override
    public void transform(PipelineChain<Integer> chain, Integer input) {
        LOG.info("Before input proceed " + chain.getName() + ", Processing " + input);
        chain.output(input * input * input);
        LOG.info("after out completed - " + chain.getName() + ", Processing " + input);
    }
}

class ChildCubeStr implements Transformer<Integer, String> {
    private static final Logger LOG = LoggerFactory.getLogger(ChildCube.class);

    @Override
    public void transform(PipelineChain<String> chain, Integer input) {
        LOG.info("Before input proceed " + chain.getName() + ", Processing " + input);
        chain.output(String.valueOf(input * input * input));
        LOG.info("after out completed - " + chain.getName() + ", Processing " + input);
    }
}

class CustomSink<T> implements PipelineSink<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomSink.class);

    private String name;

    private List<T> results = new ArrayList<>();
    private int beginCalledCount = 0;
    private int endCalledCount = 0;
    public CustomSink(String name) {
        this.name = name;
    }

    @Override
    public void onBegin() {
        LOG.info("Begin called....." + name);
        beginCalledCount++;
    }

    @Override
    public void sink(T object) {
        results.add(object);
        LOG.info("Output: " + object + " " + name);
    }

    @Override
    public void onComplete() {
        endCalledCount++;
        LOG.info("Complete called....." + name);
    }

    public List<T> getResults() {
        return results;
    }

    public int getBeginCalledCount() {
        return beginCalledCount;
    }

    public int getEndCalledCount() {
        return endCalledCount;
    }
}


class CustomSource implements PipelineSource<Integer> {

    static final List<Integer> INPUT = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9,
            10, 11, 12, 13, 14, 15, 16 ,17, 18, 19, 20);

    @Override
    public void source(PipelineChain<Integer> chain) {
        chain.onBegin();
        INPUT.stream().forEach(e -> chain.output(e));
        chain.onComplete();
    }
}

class SumOfOddsSquare implements Transformer<Integer, String> {

    private int sum = 0;
    private boolean isDownstreamStarted = false;

    @Override
    public void onBegin(PipelineChain<String> chain) {
        // Don't start immediately
    }

    @Override
    public void transform(PipelineChain chain, Integer input) {
        if (sum + input > 5000) {
            if (!isDownstreamStarted) {
                chain.onBegin();
                isDownstreamStarted = true;
            }
            chain.output(String.valueOf(sum));
            sum = 0;
        }
        sum += input;
    }

    @Override
    public void onComplete(PipelineChain chain) {
        chain.output(String.valueOf(sum));
        chain.onComplete();
    }
}
