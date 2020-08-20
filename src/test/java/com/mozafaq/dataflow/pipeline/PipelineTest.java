package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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

        ParallelOperationConfig parConfig1 =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(1)
                        .setQueueBufferSize(1)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(10)
                        .build();

        return new Object[][]{
                // Some basic test cases
                {runCase(true, true, Arrays.asList(10, 11, 12)), null},
                {runCase(true, true, Arrays.asList(10, 11, 12)), parConfig1},

                {runCase(false, true, Arrays.asList(10)), null},
                {runCase(false, true, Arrays.asList(10)), parConfig1},

                {runCase(true, false, Arrays.asList(10, 11, 12, 13, 14)), null},
                {runCase(true, false, Arrays.asList(10, 11, 12, 13, 14)), parConfig1},

                {runCase(false, false, Arrays.asList(10, 11, 12)), null},
                {runCase(false, false, Arrays.asList(10, 11, 12)), parConfig1},

                {runCase(true, true, Arrays.asList()), null},
                {runCase(true, true, Arrays.asList()), parConfig1},

                {runCase(true, false, Arrays.asList()), null},
                {runCase(true, false, Arrays.asList()), parConfig1},

                {runCase(false, true, Arrays.asList()), null},
                {runCase(false, true, Arrays.asList()), parConfig1},

                {runCase(false, false, Arrays.asList()), null},
                {runCase(false, false, Arrays.asList()), parConfig1},

        };
    }

    @DataProvider(name = "testSimplestPipelineLongRunning")
    public Object[][]  simplestPipelineDataSourceLongRunning() {

        ParallelOperationConfig parConfig2 =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(5)
                        .setQueueBufferSize(5)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(40)
                        .build();

        ParallelOperationConfig parConfig3 =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(2)
                        .setQueueBufferSize(10)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(40)
                        .build();

        ParallelOperationConfig parConfig4 =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(10)
                        .setQueueBufferSize(2)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(40)
                        .build();

        DelayConfig delayBefore = new DelayConfig(1000,1000, 1, 1);
        DelayConfig delayLater = new DelayConfig(1,1, 1000, 1000);

        return new Object[][]{

                //  delay Before config

                // Config 2
                {runCase(true, true, delayBefore, Arrays.asList(10, 11, 12, 13, 14)), parConfig2},
                {runCase(true, false, delayBefore, Arrays.asList(10, 11, 12, 13, 14)), parConfig2},
                {runCase(false, false, delayBefore, Arrays.asList(10, 11, 12)), parConfig2},
                {runCase(false, true, delayBefore, Arrays.asList(10, 11, 12)), parConfig2},
                {runCase(true, true, delayBefore, Arrays.asList()), parConfig2},

                // Config 3
                {runCase(true, true, delayBefore, Arrays.asList(10, 11, 12, 13, 14)), parConfig3},
                {runCase(true, false, delayBefore, Arrays.asList(10, 11, 12, 13, 14)), parConfig3},
                {runCase(false, false, delayBefore, Arrays.asList(10, 11, 12)), parConfig3},
                {runCase(false, true, delayBefore, Arrays.asList(10, 11, 12)), parConfig3},
                {runCase(true, true, delayBefore, Arrays.asList()), parConfig3},

                // Config 4
                {runCase(true, true, delayBefore, Arrays.asList(10, 11, 12, 13, 14)), parConfig4},
                {runCase(true, false, delayBefore, Arrays.asList(10, 11, 12, 13, 14)), parConfig4},
                {runCase(false, false, delayBefore, Arrays.asList(10, 11, 12)), parConfig4},
                {runCase(false, true, delayBefore, Arrays.asList(10, 11, 12)), parConfig4},
                {runCase(true, true, delayBefore, Arrays.asList()), parConfig4},

                //  delay After config

                // Config 2
                {runCase(true, true, delayLater, Arrays.asList(10, 11, 12, 13, 14)), parConfig2},
                {runCase(true, false, delayLater, Arrays.asList(10, 11, 12, 13, 14)), parConfig2},
                {runCase(false, false, delayLater, Arrays.asList(10, 11, 12)), parConfig2},
                {runCase(false, true, delayLater, Arrays.asList(10, 11, 12)), parConfig2},
                {runCase(true, true, delayLater, Arrays.asList()), parConfig2},

                // Config 3
                {runCase(true, true, delayLater, Arrays.asList(10, 11, 12, 13, 14)), parConfig3},
                {runCase(true, false, delayLater, Arrays.asList(10, 11, 12, 13, 14)), parConfig3},
                {runCase(false, false, delayLater, Arrays.asList(10, 11, 12)), parConfig3},
                {runCase(false, true, delayLater, Arrays.asList(10, 11, 12)), parConfig3},
                {runCase(true, true, delayLater, Arrays.asList()), parConfig3},

                // Config 4
                {runCase(true, true, delayLater, Arrays.asList(10, 11, 12, 13, 14)), parConfig4},
                {runCase(true, false, delayLater, Arrays.asList(10, 11, 12, 13, 14)), parConfig4},
                {runCase(false, false, delayLater, Arrays.asList(10, 11, 12)), parConfig4},
                {runCase(false, true, delayLater, Arrays.asList(10, 11, 12)), parConfig4},
                {runCase(true, true, delayLater, Arrays.asList()), parConfig4},

        };
    }

    private PipelineRunConfig<Integer> runCase(boolean isBeginEnabled,
                                               boolean isCompleteEnabled,
                                               List<Integer> inputEvents) {
        return runCase(
                isBeginEnabled,
                isCompleteEnabled,
                null,
                inputEvents);
    }
    private PipelineRunConfig<Integer> runCase(boolean isBeginEnabled,
                                               boolean isCompleteEnabled,
                                               DelayConfig delayConfig,
                                               List<Integer> inputEvents) {
        return  new PipelineRunConfig<Integer>(
                isBeginEnabled,
                isCompleteEnabled,
                delayConfig,
                inputEvents);
    }

    @Test(dataProvider = "testSimplestPipeline")
    public void testSimplestPipeline(PipelineRunConfig<Integer> runConfig,
                                     ParallelOperationConfig parallelOperationConfig) {

        runTestSuccess(runConfig, parallelOperationConfig);
    }

    @Test(dataProvider = "testSimplestPipelineLongRunning")
    public void testSimplestPipelineLongRunning(PipelineRunConfig<Integer> runConfig,
                                     ParallelOperationConfig parallelOperationConfig) {

        runTestSuccess(runConfig, parallelOperationConfig);
    }

    public void runTestSuccess(PipelineRunConfig<Integer> runConfig, ParallelOperationConfig parallelOperationConfig) {
        TestPipeline creator = new TestPipeline(parallelOperationConfig, runConfig, new ExceptionConfig());
        Pipeline pipeline = creator.getPipeline();
        pipeline.run();

        List<Integer> resultCube = runConfig.getEvents()
                .stream().map(e -> e * e * e).collect(Collectors.toUnmodifiableList());
        List<String> resultSquare = runConfig.getEvents()
                .stream().map(e -> String.valueOf(e * e)).collect(Collectors.toUnmodifiableList());

        assertEquals(creator.getCustomSinkCube().getBeginCalledCount(), runConfig.isBeginCall() ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getEndCalledCount(), runConfig.isCompleteCall() ? 1 : 0);
        assertEquals(creator.getCustomSinkCube().getResults(), resultCube);
        assertEquals(creator.getCustomSinkSquare().getBeginCalledCount(), runConfig.isBeginCall() ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getEndCalledCount(), runConfig.isCompleteCall() ? 1 : 0);
        assertEquals(creator.getCustomSinkSquare().getResults(), resultSquare);
    }

    @DataProvider(name = "testSimplestPipelineExceptions")
    public Object[][]  simplestPipelineDataSourceExceptions() {

        ParallelOperationConfig parConfigGeneral =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(5)
                        .setQueueBufferSize(5)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(10)
                        .build();

        ParallelOperationConfig parConfigForTimeout =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(10)
                        .setQueueBufferSize(2)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(3)
                        .build();

        ParallelOperationConfig parConfigForSingleBufferSize =
                ParallelOperationConfig.newBuilder()
                        .setEventBatchSize(1)
                        .setQueueBufferSize(1)
                        .setQueuePollDuration(Duration.ofMillis(1000))
                        .setCountForInsertAttempt(3)
                        .build();

        DelayConfig delayLater =
                new DelayConfig(1,1, 5000, 5000);

        PipelineRunConfig<Integer> with3EventsWithoutDelay =
                runCase(true, true, Arrays.asList(10, 11, 12));

        PipelineRunConfig<Integer> with3EventsWithLateDelay =
                runCase(true, true, delayLater, Arrays.asList(10, 11, 12));

        PipelineRunConfig<Integer> with3EventsWithoutDelayNoBeginEnd =
                runCase(false, false, Arrays.asList(10, 11, 12));

        ExceptionConfig beforeExceptionStr =
                new ExceptionConfig(null,Set.of("11"), null, null);
        ExceptionConfig beforeExceptionInt =
                new ExceptionConfig(Set.of(12),null, null, null);
        ExceptionConfig afterExceptionInt =
                new ExceptionConfig(null, null, Set.of(121), null);
        ExceptionConfig afterExceptionStr =
                new ExceptionConfig(null, null, null,Set.of("1331"));
        ExceptionConfig afterMultipleExceptions =
                new ExceptionConfig(null, null,  Set.of(100), Set.of("1331"));

        ExceptionConfig beginException =
                new ExceptionConfig(true, false);

        ExceptionConfig endException =
                new ExceptionConfig(false, true);

        ExceptionConfig noException = new ExceptionConfig();

        return new Object[][]{

                //  Exceptions
                {with3EventsWithoutDelay, beforeExceptionInt, parConfigGeneral},
                {with3EventsWithoutDelay, beforeExceptionStr, parConfigGeneral},
                {with3EventsWithoutDelay, afterExceptionInt, parConfigGeneral},
                {with3EventsWithoutDelay, afterExceptionStr, parConfigGeneral},
                {with3EventsWithoutDelay, afterExceptionStr, parConfigGeneral},
                {with3EventsWithoutDelay, afterMultipleExceptions, parConfigGeneral},

                {with3EventsWithoutDelay, beginException, parConfigGeneral},
                {with3EventsWithoutDelay, endException, parConfigGeneral},

                {with3EventsWithoutDelayNoBeginEnd, beforeExceptionInt, parConfigGeneral},
                {with3EventsWithoutDelayNoBeginEnd, beforeExceptionStr, parConfigGeneral},
                {with3EventsWithoutDelayNoBeginEnd, afterExceptionInt, parConfigGeneral},
                {with3EventsWithoutDelayNoBeginEnd, afterExceptionStr, parConfigGeneral},
                {with3EventsWithoutDelayNoBeginEnd, afterExceptionStr, parConfigGeneral},
                {with3EventsWithoutDelayNoBeginEnd, afterMultipleExceptions, parConfigGeneral},

                {with3EventsWithLateDelay, noException, parConfigForSingleBufferSize},
                {with3EventsWithLateDelay, noException, parConfigForTimeout},
        };
    }

    @Test(dataProvider = "testSimplestPipelineExceptions", expectedExceptions = IllegalStateException.class)
    public void testPipelineForExceptions(PipelineRunConfig<Integer> config,
                                          ExceptionConfig exceptionConfig,
                                          ParallelOperationConfig parallelConfig)
    throws Throwable {
        TestPipeline creator = new TestPipeline(parallelConfig, config, exceptionConfig);
        Pipeline pipeline = creator.getPipeline();

        try {
            pipeline.run();
        } catch (Exception e) {
            throw e.getCause();
        }
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

            private int beginCount = 0;
            private int completeCount = 0;
            @Override
            public void onBegin() {
                beginCount++;
                LOG.info("Sink - Begin");
            }
            @Override
            public void onComplete() {
                LOG.info("Sink - Complete");
            }

            @Override
            public void sink(Integer val) {
                completeCount++;
                LOG.info("Sink value - " + val);
            }

            public int getBeginCount() {
                return beginCount;
            }

            public int getCompleteCount() {
                return completeCount;
            }
        }

        Pipeline pipeline = Pipeline.create();
        PipelineEventState<Integer> source = pipeline.fromSource("Source", new TestSource());
        TestSink sink1 = new TestSink();
        TestSink sink2 = new TestSink();
        source.sink("Sink1", sink1);
        source.sink("Sink2", sink2);
        pipeline.build();
        pipeline.run();
        assertEquals(sink1.getBeginCount(), 1);
        assertEquals(sink1.getCompleteCount(), 1);
        assertEquals(sink2.getBeginCount(), 1);
        assertEquals(sink2.getCompleteCount(), 1);
    }
}
