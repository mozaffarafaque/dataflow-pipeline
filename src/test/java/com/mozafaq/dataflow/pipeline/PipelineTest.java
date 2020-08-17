package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
        PipelineData<Integer> dataInts = pipeline.fromSource("Source", new CustomSource());
        PipelineData<Integer> dataIntThrice = dataInts.addTransformer("IntegerThrice", (a, b) -> a.output(3 * b));
        PipelineData<Integer> dataIntSquares = dataIntThrice.addTransformer("IntegerSquare", (a, b) -> a.output(b * b));
        PipelineData<Integer> dataSquareEvents = dataIntSquares.addTransformer("IntSquareEven", (a, b) ->
        {
            if (b % 2 == 0) {
                a.output(b);
            }
        });

        PipelineData<Integer> dataSquareOdds = dataIntSquares.addTransformer("IntSquareOdd", (a, b) ->
        {
            if (b % 2 == 1) {
                a.output(b);
            }
        });


        PipelineData<String> dataSquareEvenStr =
                dataSquareEvents.addTransformer("SquareEvenString", (a, b) -> a.output(String.valueOf(b)));

        PipelineData<String> dataSquareOddsStr =
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

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void testExceptionWithoutCreation() {
        Pipeline pipeline = Pipeline.create();
        pipeline.run();
    }

    @DataProvider(name = "testSimplestPipeline")
    public Object[][]  simplestPipelineDataSource() {
        ConcurrentTransformerConfig concurrentTransformerConfig =
                new ConcurrentTransformerConfig(
                        1,
                        1,
                        1000,
                        1000
                );
        return new Object[][] {
                {null},
               // {concurrentTransformerConfig}
        };
    }


    @Test(dataProvider = "testSimplestPipeline")
    public void testSimplestPipeline(ConcurrentTransformerConfig concurrentTransformerConfig) {
        TestSimplestPipelineCreator creator = new TestSimplestPipelineCreator(concurrentTransformerConfig);
        Pipeline pipeline = creator.getPipeline();

        pipeline.run();
//
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        assertEquals(creator.getCustomSinkCube().getBeginCalledCount(), 1);
        assertEquals(creator.getCustomSinkCube().getEndCalledCount(), 1);
        assertEquals(creator.getCustomSinkCube().getResults(), Arrays.asList(1000, 1331, 1728));
        assertEquals(creator.getCustomSinkSquare().getBeginCalledCount(), 1);
        assertEquals(creator.getCustomSinkSquare().getEndCalledCount(), 1);
        assertEquals(creator.getCustomSinkSquare().getResults(), Arrays.asList(100, 121, 144));

       // System.out.println("Finish-----------");
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
