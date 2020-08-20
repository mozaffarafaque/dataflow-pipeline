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
                        .map(a -> 9 * a * a )
                        .filter(a -> a % 2 == 0)
                        .collect(Collectors.toUnmodifiableList()));

        assertEquals(valueSinkEvens,
                CustomSource.INPUT
                        .stream()
                        .map(a -> 9 * a * a)
                        .filter(a -> a % 2 == 0)
                        .map(a -> String.valueOf(a))
                        .collect(Collectors.toUnmodifiableList())
        );
        assertEquals(valueSinkOddsAggregated, Arrays.asList("4095", "4626", "3249"));
    }

    private Pipeline createPipeline(List<String> valueSinkEvens,
                                    List<String> valueSinkOddsAggregated,
                                    List<Integer> valueSinkEvenInt) {

        Pipeline pipeline = Pipeline.create();
        PipelineEventState<Integer> dataInts = pipeline.fromSource("Source", new CustomSource());
        PipelineEventState<Integer> dataIntThrice = dataInts.addTransformer("IntegerThrice", (a, b) -> a.output(3 * b));
        PipelineEventState<Integer> dataIntSquares = dataIntThrice.addTransformer("IntegerSquare", (a, b) -> a.output(b * b));
        PipelineEventState<Integer> dataSquareEvents = dataIntSquares.addTransformer("IntSquareEven", (a, b) ->
        {
            if (b % 2 == 0) {
                a.output(b);
            }
        });

        PipelineEventState<Integer> dataSquareOdds = dataIntSquares.addTransformer("IntSquareOdd", (a, b) ->
        {
            if (b % 2 == 1) {
                a.output(b);
            }
        });


        PipelineEventState<String> dataSquareEvenStr =
                dataSquareEvents.addTransformer("SquareEvenString", (a, b) -> a.output(String.valueOf(b)));

        PipelineEventState<String> dataSquareOddsStr =
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
        PipelineEventState<Integer> source =
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
                        .setCountForInsertAttempt(10)
                        .build();

        return new Object[][]{
                {runCase(true, true, Arrays.asList(10, 11, 12)), null},
                {runCase(true, true, Arrays.asList(10, 11, 12)), parallelOperationConfig},

                {runCase(false, true, Arrays.asList(10)), null},
                {runCase(false, true, Arrays.asList(10)), parallelOperationConfig},

                {runCase(true, false, Arrays.asList(10, 11, 12, 13, 14)), null},
                {runCase(true, false, Arrays.asList(10, 11, 12, 13, 14)), parallelOperationConfig},

                {runCase(false, false, Arrays.asList(10, 11, 12)), null},
                {runCase(false, false, Arrays.asList(10, 11, 12)), parallelOperationConfig},

                {runCase(true, true, Arrays.asList()), null},
                {runCase(true, true, Arrays.asList()), parallelOperationConfig},

                {runCase(true, false, Arrays.asList()), null},
                {runCase(true, false, Arrays.asList()), parallelOperationConfig},

                {runCase(false, true, Arrays.asList()), null},
                {runCase(false, true, Arrays.asList()), parallelOperationConfig},

                {runCase(false, false, Arrays.asList()), null},
                {runCase(false, false, Arrays.asList()), parallelOperationConfig},
        };
    }

    private PipelineRunConfig<Integer> runCase(boolean isBeginEnabled,
                                               boolean isCompleteEnabled,
                                               List<Integer> inputEvents) {
        return runCase(
                isBeginEnabled,
                isCompleteEnabled,
                1,
                1,
                inputEvents);
    }
    private PipelineRunConfig<Integer> runCase(boolean isBeginEnabled,
                                               boolean isCompleteEnabled,
                                               long beforeRecordOutWait,
                                               long afterRecordOutWait,
                                               List<Integer> inputEvents) {
        return  new PipelineRunConfig<Integer>(
                isBeginEnabled,
                isCompleteEnabled,
                beforeRecordOutWait,
                afterRecordOutWait,
                inputEvents);
    }

    @Test(dataProvider = "testSimplestPipeline")
    public void testSimplestPipeline(PipelineRunConfig<Integer> runConfig,
                                     ParallelOperationConfig parallelOperationConfig) {


        TestSimplestPipelineCreator creator = new TestSimplestPipelineCreator(parallelOperationConfig, runConfig);
        Pipeline pipeline = creator.getPipeline();
        pipeline.run();

        List<Integer> resultCube = runConfig.getInputEvents().stream().map(e -> e * e * e).collect(Collectors.toUnmodifiableList());
        List<Integer> resultSquare = runConfig.getInputEvents().stream().map(e -> e * e).collect(Collectors.toUnmodifiableList());

        assertEquals(creator.getCustomSinkCube().getBeginCalledCount(), runConfig.isBeginEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getEndCalledCount(), runConfig.isCompleteEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getResults(), resultCube);
        assertEquals(creator.getCustomSinkSquare().getBeginCalledCount(), runConfig.isBeginEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getEndCalledCount(), runConfig.isCompleteEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getResults(), resultSquare);
    }

    @Test(dataProvider = "testSimplestPipeline")
    public void testSimplestPipeline2(PipelineRunConfig<Integer> runConfig,
                                     ParallelOperationConfig parallelOperationConfig) {
        
        TestPipelineDataTypeExchange creator = new TestPipelineDataTypeExchange(parallelOperationConfig, runConfig);
        Pipeline pipeline = creator.getPipeline();
        pipeline.run();

        List<Integer> resultCube = runConfig.getInputEvents()
                .stream().map(e -> e * e * e).collect(Collectors.toUnmodifiableList());
        List<String> resultSquare = runConfig.getInputEvents()
                .stream().map(e -> String.valueOf(e * e)).collect(Collectors.toUnmodifiableList());

        assertEquals(creator.getCustomSinkCube().getBeginCalledCount(), runConfig.isBeginEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getEndCalledCount(), runConfig.isCompleteEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getResults(), resultCube);
        assertEquals(creator.getCustomSinkSquare().getBeginCalledCount(), runConfig.isBeginEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getEndCalledCount(), runConfig.isCompleteEnabled() ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getResults(), resultSquare);
    }

    @Test
    public void test3NodeForkAtRoot() {
        class TestSource implements PipelineSource<Integer> {
            private final Logger LOG = LoggerFactory.getLogger(TestSource.class);
            @Override
            public void source(PipelineChain<Integer> chain) {
                LOG.info("Starting at source");
                chain.onBegin();
                chain.output(100);
                chain.onComplete();
                LOG.info("Completion at source");
            }
        }

        class TestSink implements PipelineSink<Integer> {
            private final Logger LOG = LoggerFactory.getLogger(TestSink.class);

            @Override
            public void onBegin() {
                LOG.info("Sink - Begin");
            }
            @Override
            public void onComplete() {
                LOG.info("Sink - Complete");
            }

            @Override
            public void sink(Integer val) {
                LOG.info("Starting at Sink");
                LOG.info("Sink value - " + val);
                LOG.info("Completion at Sink");
            }
        }

        Pipeline pipeline = Pipeline.create();
        PipelineEventState<Integer> source = pipeline.fromSource("Source", new TestSource());
        source.sink("Sink1", new TestSink());
        source.sink("Sink2", new TestSink());
        pipeline.build();
        pipeline.run();
    }
}
