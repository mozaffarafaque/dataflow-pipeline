# dataflow-pipeline - Dataflow pipeline of bounded stream

## Summary

Supposed to process bounded set of data in streamed fashion through various 
steps defined in pipeline workflow. Process can be transformation from X to Y.
This is supposed to run on single host.

## Terminologies

 **Dataflow Pipeline:** It can be considered as a directed acyclic tree. 
 Root node is source and leaf nodes are sinks. A node in the graph are 
 considered to have collection of transformers. No. of transformers in 
 the node is equal to the no. of outgoing edges from node.
 
 **Transformer:** It is transformation logic - application of the 
 transformation logic on one or more input events  produces 
 zero or more new events.
 
 **Pipeline State:** A node in the graph can be termed as `Pipeline State`. 
 This is always associated with a transformer. It is a representation 
 of node in the graph. A state node is the representation of node that
  has been created by **applying the associated transformer.** 
 
 **Pipeline Chain:** A handler to transfer the event from the output of a 
 transformer to next in the pipeline.
 

## Dataflow Pipeline Usage
 
  Allows to create pipeline, a pipeline can be considered
  as a directed tree with root node as source of the data and leaf nodes as sink.
  Intermediate nodes can be thought of 
  transformers and edges represent data or stream of data. 
  One can provide own pipeline definition by implementing interface 
  `com.mozafaq.dataflow.pipeline.PipelineCreateAware`.
  
  A source, transformer and a sink objects can be created by 
   implementing below interfaces.
   - **`com.mozafaq.dataflow.pipeline.PipelineSource`**: Similar to
   `Transformer` except this is the beginning og the pipeline.
   If you want pipeline to receive `onBegin` and `onComplete` event then you must 
   call being and end event before and  after processing source.
    
   - **`com.mozafaq.dataflow.pipeline.Transformer`**: This has below APIs 
   and their purposes
      - `onBegin`: Called only once in the beginning, optional to implement. If you 
      implement it it must call `pipelinechain.onBegin()` so that underlying 
      pipeline flow can get onBegin event.
      - `transform`: This is where actual logic reside. This should call 
      `chain.output` for sending output for subsequent processing.
      - `onComplete`: Called only once in the beginning, optional to implement. If you 
      implement it it must call `pipelinechain.onComplete()` so that underlying 
      pipeline flow can get onComplete event.
      
   - **`com.mozafaq.dataflow.pipeline.PipelineSink`**:
     Similar to `Transformer` except that this is last in the pipeline therefore you don't
      have option to sent event further down in the pipeline.
    
  An example graph of pipeline can be seen as below
  
  
  ![alt text](etc/sample-pipeline.png)
  
  This pipeline does followings
   1. Gets the large file
   1. Sort the file
   1. Read the record on-by-one from sorted file and convert into event
   1. Events are forwarded to two path -> Aggregation and Conversion
   1. Aggregator aggregate record and Converter convert the events into different format
   1. Aggregated record is published at some place
   1. Converted records are persisted
  
### Developer Note

As mentioned above, the node in the pipeline is collection of 
transformers. However, if a transformer that is performing 
intensive operation and that node has more than one outgoing edges
then it is advised to have an identity node added after the 
computation-intensive/IO-intensive node for performance reasons.
This will avoid this operation not too happen multiple times.
Typically, source is a heavy operation therefore while creating the source
from pipeline, it adds an identity node always. 
## Build

 ```mvn clean insall```

## Contributors

 - Mozaffar Afaque