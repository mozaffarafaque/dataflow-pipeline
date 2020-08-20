package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Mozaffar Afaque
 */
public class TestPipeline implements PipelineCreateAware {

    private static final Logger LOG = LoggerFactory.getLogger(TestPipeline.class);

    private CustomSink customSinkSquare = new CustomSink("Square");
    private CustomSink customSinkCube = new CustomSink("Cube");
    private PipelineRunConfig<Integer> runConfig;
    private ExceptionConfig exceptionConfig ;

    private ParallelOperationConfig parallelOperationConfig;

    public TestPipeline(ParallelOperationConfig parallelOperationConfig, PipelineRunConfig runConfig, ExceptionConfig exceptionConfig) {
        this.parallelOperationConfig = parallelOperationConfig;
        this.runConfig = runConfig;
        this.exceptionConfig = exceptionConfig;
    }

    @Override
    public Pipeline getPipeline() {

        Pipeline pipeline = Pipeline.create();
        Source source = new Source(
                runConfig.isBeginCall(),
                runConfig.isCompleteCall(),
                runConfig.getEvents());


        IntToStrIdentity identityTransformerIntToStr1 =
                new IntToStrIdentity(runConfig.getDelayConfig().getDelayBefore1(),
                        runConfig.getDelayConfig().getDelayAfter1());
        identityTransformerIntToStr1.setExceptionEvents(exceptionConfig.getBeforeEventsInt());

        StrToIntIdentity identityTransformerStrToInt1 =
                new StrToIntIdentity(runConfig.getDelayConfig().getDelayBefore1(),
                        runConfig.getDelayConfig().getDelayAfter1());
        identityTransformerStrToInt1.setExceptionEvents(exceptionConfig.getBeforeEventsStr());

        IntToStrIdentity identityTransformerIntToStr2 =
                new IntToStrIdentity(runConfig.getDelayConfig().getDelayBefore2(),
                        runConfig.getDelayConfig().getDelayAfter2());
        identityTransformerIntToStr2.setExceptionEvents(exceptionConfig.getLaterEventsInt());
        identityTransformerIntToStr2.setExceptionOnBegin(exceptionConfig.isExceptionOnBegin());
        identityTransformerIntToStr2.setExceptionOnComplete(exceptionConfig.isExceptionOnComplete());

        StrToIntIdentity identityTransformerStrToInt2 =
                new StrToIntIdentity(runConfig.getDelayConfig().getDelayBefore2(),
                        runConfig.getDelayConfig().getDelayAfter2());
        identityTransformerStrToInt2.setExceptionEvents(exceptionConfig.getLaterEventsStr());
        identityTransformerStrToInt2.setExceptionOnBegin(exceptionConfig.isExceptionOnBegin());
        identityTransformerStrToInt2.setExceptionOnComplete(exceptionConfig.isExceptionOnComplete());

        PipelineEventState<Integer> intEventsFromSource = pipeline.fromSource("SourceInt", source);

        PipelineEventState<String> identicalEventsAsSourceStr =
                (parallelOperationConfig != null ?
                        intEventsFromSource.addParallelTransformer("IdenticalAsSourceStr", identityTransformerIntToStr1, parallelOperationConfig)
                        : intEventsFromSource.addTransformer("IdenticalAsSourceStr", identityTransformerIntToStr1));

        PipelineEventState<Integer> identicalEventsAsSourceInt =
                (parallelOperationConfig != null ?
                        identicalEventsAsSourceStr.addParallelTransformer("IdenticalAsSourceInt", identityTransformerStrToInt1, parallelOperationConfig)
                        : identicalEventsAsSourceStr.addTransformer("IdenticalAsSourceInt", identityTransformerStrToInt1));

        PipelineEventState<Integer> square =
                (parallelOperationConfig != null ?
                        identicalEventsAsSourceInt.addParallelTransformer("ChildSquare", new ChildSquare(), parallelOperationConfig)
                        : identicalEventsAsSourceInt.addTransformer("ChildSquare", new ChildSquare()));

        PipelineEventState<String> cube =
                (parallelOperationConfig != null ?
                        identicalEventsAsSourceInt.addParallelTransformer("ChildCubeStr", new ChildCubeStr(), parallelOperationConfig)
                        : identicalEventsAsSourceInt.addTransformer("ChildCubeStr", new ChildCubeStr()));

        PipelineEventState<String> identicalSquareStr =
                (parallelOperationConfig != null ?
                        square.addParallelTransformer("IdenticalAsSquare", identityTransformerIntToStr2, parallelOperationConfig)
                        : square.addTransformer("IdenticalAsSquare", identityTransformerIntToStr2));

        PipelineEventState<Integer> identicalCubeInt =
                (parallelOperationConfig != null ?
                        cube.addParallelTransformer("IdenticalAsCubeInt", identityTransformerStrToInt2, parallelOperationConfig)
                        : cube.addTransformer("IdenticalAsCubeInt", identityTransformerStrToInt2));

        identicalSquareStr.sink("SquareSink", customSinkSquare);
        identicalCubeInt.sink("CubeSink", customSinkCube);
        pipeline.build();


        LOG.info("Identities: identityTransformerIntToStr1: " + identityTransformerIntToStr1 +
                "\nidentityTransformerStrToInt1:" + identityTransformerStrToInt1 +
                "\nidentityTransformerIntToStr2:" + identityTransformerIntToStr2 +
                "\nidentityTransformerStrToInt2:" + identityTransformerStrToInt2);

        return pipeline;
    }


    public CustomSink getCustomSinkSquare() {
        return customSinkSquare;
    }

    public CustomSink getCustomSinkCube() {
        return customSinkCube;
    }
}
