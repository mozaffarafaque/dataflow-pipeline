package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

/**
 * @author Mozaffar Afaque
 */
public class PipelineTest {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineTest.class);

    @Test
    public void testPipelineFlow() {
        List<String> valueSinkOddsAggregated = new ArrayList<>();
        List<String> valueSinkEvens = new ArrayList<>();
        List<Integer> valueSinkEvenInts = new ArrayList<>();
        Pipeline pipeline = createPipeline(valueSinkEvens, valueSinkOddsAggregated, valueSinkEvenInts);
        pipeline.run();

        assertEquals(valueSinkEvenInts,
                CustomSource.INPUT
                        .stream()
                        .map(a -> 9*a*a )
                        .filter(a -> a%2 == 0)
                        .collect(Collectors.toUnmodifiableList()));

        assertEquals(valueSinkEvens,
                CustomSource.INPUT
                        .stream()
                        .map(a -> 9*a*a)
                        .filter(a -> a%2 == 0)
                        .map(a -> String.valueOf(a))
                        .collect(Collectors.toUnmodifiableList())
        );
        assertEquals(valueSinkOddsAggregated, Arrays.asList("4095", "4626", "3249"));
    }

    private Pipeline createPipeline(List<String> valueSinkEvens,
                                    List<String> valueSinkOddsAggregated,
                                    List<Integer> valueSinkEvenInt) {

        Pipeline pipeline = Pipeline.create();
        PipelineState<Integer> dataInts = pipeline.fromSource("Source", new CustomSource());
        PipelineState<Integer> dataIntThrice = dataInts.addTransformer("IntegerThrice", (a, b) -> a.output(3 * b));
        PipelineState<Integer> dataIntSquares = dataIntThrice.addTransformer("IntegerSquare", (a, b) -> a.output(b * b));
        PipelineState<Integer> dataSquareEvents = dataIntSquares.addTransformer("IntSquareEven", (a, b) ->
        {
            if (b % 2 == 0) {
                a.output(b);
            }
        });

        PipelineState<Integer> dataSquareOdds = dataIntSquares.addTransformer("IntSquareOdd", (a, b) ->
        {
            if (b % 2 == 1) {
                a.output(b);
            }
        });


        PipelineState<String> dataSquareEvenStr =
                dataSquareEvents.addTransformer("SquareEvenString", (a, b) -> a.output(String.valueOf(b)));

        PipelineState<String> dataSquareOddsStr =
                dataSquareOdds.addTransformer("SumOfOddsSquare", new SumOfOddsSquare());

        dataSquareEvenStr.sink("SinkEvenStr", a -> valueSinkEvens.add(a));
        dataSquareOddsStr.sink("SinkOdd", a -> valueSinkOddsAggregated.add(a));
        dataSquareEvents.sink("SinkEven", a -> valueSinkEvenInt.add(a));

        pipeline.build();
        return pipeline;

    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void testExceptionWithoutInitialize() {
        Pipeline pipeline = Pipeline.create();
        pipeline.run();
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testExceptionMultipleBuild() {
        Pipeline pipeline = Pipeline.create();
        pipeline.fromSource("Test" , a -> a.output(100));
        pipeline.build();
        pipeline.build();
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testExceptionMultipleRoot() {
        Pipeline pipeline = Pipeline.create();
        pipeline.fromSource("Test1" , a -> a.output(100));
        pipeline.fromSource("Test2" , a -> a.output(200));
    }

    @Test(expectedExceptions = {NullPointerException.class})
    public void testExceptionWithNullSource() {
        Pipeline pipeline = Pipeline.create();
        pipeline.fromSource(null, null);
    }

    @Test(expectedExceptions = {NullPointerException.class})
    public void testExceptionNullParallelConfig() {
        Pipeline pipeline = Pipeline.create();
        PipelineState<Integer> source =
                pipeline.fromSource("Test1" , a -> a.output(100));
        source.addParallelTransformer("NullConfig", (a, b) -> a.output(b), null);
    }


    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void testExceptionWithoutCreation() {
        Pipeline pipeline = Pipeline.create();
        pipeline.run();
    }

    @DataProvider(name = "testSimplestPipeline")
    public Object[][]  simplestPipelineDataSource() {

        ParallelOperationConfig parallelOperationConfig =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(1)
                        .setQueueBufferSize(1)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(1000)
                        .build();

        return new Object[][]{
                {true, true, Arrays.asList(10, 11, 12), null},
                {true, true, Arrays.asList(10, 11, 12), parallelOperationConfig},

                {false, true, Arrays.asList(10, 11, 12), null},
                {false, true, Arrays.asList(10, 11, 12), parallelOperationConfig},

                {true, false, Arrays.asList(10, 11, 12), null},
                {true, false, Arrays.asList(10, 11, 12), parallelOperationConfig},

                {false, false, Arrays.asList(10, 11, 12), null},
                {false, false, Arrays.asList(10, 11, 12), parallelOperationConfig},

                {true, true, Arrays.asList(), null},
                {true, true, Arrays.asList(), parallelOperationConfig},

                {true, false, Arrays.asList(), null},
                {true, false, Arrays.asList(), parallelOperationConfig},

                {false, true, Arrays.asList(), null},
                {false, true, Arrays.asList(), parallelOperationConfig},

                {false, false, Arrays.asList(), null},
                {false, false, Arrays.asList(), parallelOperationConfig},
        };
    }
    
    @Test(dataProvider = "testSimplestPipeline")
    public void testSimplestPipeline(boolean isBeginEvent,
                                     boolean isEndEvent,
                                     List<Integer> events,
                                     ParallelOperationConfig parallelOperationConfig) {

        Source source = new Source(isBeginEvent, isEndEvent, events);

        TestSimplestPipelineCreator creator = new TestSimplestPipelineCreator(parallelOperationConfig, source);
        Pipeline pipeline = creator.getPipeline();
        pipeline.run();

        List<Integer> resultCube = events.stream().map(e -> e * e * e).collect(Collectors.toUnmodifiableList());
        List<Integer> resultSquare = events.stream().map(e -> e * e).collect(Collectors.toUnmodifiableList());

        assertEquals(creator.getCustomSinkCube().getBeginCalledCount(), isBeginEvent ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getEndCalledCount(), isEndEvent ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getResults(), resultCube);
        assertEquals(creator.getCustomSinkSquare().getBeginCalledCount(), isBeginEvent ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getEndCalledCount(), isEndEvent ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getResults(), resultSquare);
    }
}

class CustomSource implements PipelineSource<Integer> {

    static final List<Integer> INPUT = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9,
            10, 11, 12, 13, 14 ,15 ,16 ,17, 18, 19, 20);

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
