package datapipes.pipeline.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Mozaffar Afaque
 */
public class TestSimplestPipelineCreator implements PipelineCreateAware {
    private CustomSink customSinkSquare = new CustomSink("Square");
    private CustomSink customSinkCube = new CustomSink("Cube");

    @Override
    public Pipeline getPipeline() {

      Pipeline pipeline =  Pipeline.create();

      PipelineData<Integer> intData1 =  pipeline.fromSource("Source", new Source());
      PipelineData<Integer> intData = intData1.addTransformer("Dummy Node" , (a,b) -> a.output(b) );

      PipelineData<Integer> square = intData.addTransformer("Child Square" , new ChildSquare() );
      PipelineData<Integer> cube = intData.addTransformer("Child Cube" , new ChildCube() );

      square.sink("Square sink", customSinkSquare);
      cube.sink("Cube sink", customSinkCube);
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


class Source implements PipelineSource<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    @Override
    public void source(PipelineChain<Integer> chain) {

        LOG.info("Starting at source ");
        chain.onBegin();
        LOG.info("Starting at source begin completed ");
        chain.output(10);
        chain.output(11);
        chain.output(12);
        LOG.info("Starting at source out completed ");

        chain.onComplete();
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

class CustomSink implements PipelineSink<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomSink.class);

    private String name;

    private List<Integer> results = new ArrayList<>();
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
    public void sink(Integer object) {
        results.add(object);
        LOG.info("Output: " + object + " " + name);
    }

    @Override
    public void onComplete() {
        endCalledCount++;
        LOG.info("Complete called....." + name);
    }

    public List<Integer> getResults() {
        return results;
    }

    public int getBeginCalledCount() {
        return beginCalledCount;
    }

    public int getEndCalledCount() {
        return endCalledCount;
    }
}