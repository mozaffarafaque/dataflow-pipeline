package datapipes.pipeline.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Mozaffar Afaque
 */
public class PipelineChainImpl<T> implements PipelineChain<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineChainImpl.class);
    private Transformer transformer;
    private List<PipelineChain> chains;
    private String name;

    public static final PipelineChain PIPELINE_CHAIN = new PipelineChain() {
        @Override
        public void output(Object out) {}
        @Override
        public void onComplete() {}

        @Override
        public String getName() {
            return "<Sink>";
        }

        @Override
        public void onBegin() {

        }
    };

    PipelineChainImpl(String name, Transformer transformer, List<PipelineChain> chains) {
        this.name = name;
        this.transformer = transformer;
        this.chains = chains;
    }

    @Override
    public void output(T out) {
        for (PipelineChain chain : chains) {
            transformer.transform(chain, out);
        }
    }

    @Override
    public void onComplete() {
        for (PipelineChain chain : chains) {
            transformer.onComplete(chain);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void onBegin() {
        for (PipelineChain chain : chains) {
            transformer.onBegin(chain);
        }
    }
};
