package com.mozafaq.dataflow.pipeline;

public class TestPipelineDataTypeExchange implements PipelineCreateAware {

    private CustomSink customSinkSquare = new CustomSink("Square");
    private CustomSink customSinkCube = new CustomSink("Cube");
    PipelineRunConfig<Integer> runConfig;

    private ParallelOperationConfig parallelOperationConfig;

    public TestPipelineDataTypeExchange(ParallelOperationConfig parallelOperationConfig, PipelineRunConfig runConfig) {
        this.parallelOperationConfig = parallelOperationConfig;
        this.runConfig = runConfig;
    }

    @Override
    public Pipeline getPipeline() {

        Pipeline pipeline = Pipeline.create();
        Source source = new Source(runConfig.isBeginEnabled(), runConfig.isCompleteEnabled(), runConfig.getInputEvents());

        Transformer<Integer, String> identityTransformerIntToStr = new IntToStrIdentity();

        Transformer<String, Integer> identityTransformerStrToInt = new StrToIntIdentity();

        Transformer<String, String> identityTransformerStrToStr = new StrToStrIdentity();

        Transformer<Integer, Integer> identityTransformerIntToInt = new IntToIntIdentity();

        PipelineEventState<Integer> intEventsFromSource = pipeline.fromSource("SourceInt", source);

        PipelineEventState<String> identicalEventsAsSourceStr =
                (parallelOperationConfig != null ?
                        intEventsFromSource.addParallelTransformer("IdenticalAsSourceStr", identityTransformerIntToStr, parallelOperationConfig)
                        : intEventsFromSource.addTransformer("IdenticalAsSourceStr", identityTransformerIntToStr));

        PipelineEventState<Integer> identicalEventsAsSourceInt =
                (parallelOperationConfig != null ?
                        identicalEventsAsSourceStr.addParallelTransformer("IdenticalAsSourceInt", identityTransformerStrToInt, parallelOperationConfig)
                        : identicalEventsAsSourceStr.addTransformer("IdenticalAsSourceInt", identityTransformerStrToInt));

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
                        square.addParallelTransformer("IdenticalAsSquare", identityTransformerIntToStr, parallelOperationConfig)
                        : square.addTransformer("IdenticalAsSquare", identityTransformerIntToStr));

        PipelineEventState<Integer> identicalCubeInt =
                (parallelOperationConfig != null ?
                        cube.addParallelTransformer("IdenticalAsCubeInt", identityTransformerStrToInt, parallelOperationConfig)
                        : cube.addTransformer("IdenticalAsCubeInt", identityTransformerStrToInt));

        identicalSquareStr.sink("Square sink", customSinkSquare);
        identicalCubeInt.sink("Cube sink", customSinkCube);
        pipeline.build();
        return pipeline;
    }


    public CustomSink getCustomSinkSquare() {
        return customSinkSquare;
    }

    public CustomSink getCustomSinkCube() {
        return customSinkCube;
    }
}
