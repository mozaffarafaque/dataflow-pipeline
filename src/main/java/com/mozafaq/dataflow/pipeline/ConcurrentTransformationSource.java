package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;


public class ConcurrentTransformationSource<I, O> implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentTransformationSource.class);

    // For handling the data transfer and possible exceptions.
    private ArrayBlockingQueue<List<I>> arrayBlockingQueue;
    private Exception exception;
    private transient Thread runningThread;

    // Data pipeline related ones
    final private Transformer<I, O> transformer;
    final private PipelineChain<O> chain;
    final private ConcurrentTransformerConfig concurrentTransformerConfig;

    private List<I> eventBatch;

    // Supplementary data
    private boolean isStarted;
    private boolean operationCompleted;
    private long recordOut;
    private long recordIn;
    private long recordInCounter;

    // Monitors
    private final WaitMonitor waitMonitorBegin;
    private final WaitMonitor waitMonitorComplete;

    public ConcurrentTransformationSource(Transformer<I, O> transformer, PipelineChain<O> chain, ConcurrentTransformerConfig concurrentTransformerConfig) {
        this.transformer = transformer;
        this.chain = chain;
        this.concurrentTransformerConfig = concurrentTransformerConfig;

        this.isStarted = false;
        this.operationCompleted = false;
        this.recordOut = 0;
        this.recordIn = -1;
        this.recordInCounter = 0;

        this.waitMonitorBegin = new WaitMonitor();
        this.waitMonitorComplete = new WaitMonitor();

        this.eventBatch = new ArrayList<>(concurrentTransformerConfig.getEventBatchSize());
    }

    @Override
    public synchronized void run() {

        LOG.info("Thread running....");
        isStarted = true;
        boolean completed = recordIn == recordOut;
        try {
            onBeginHandler();
            LOG.info("Begin signal received....");

            while (!completed) {
                List<I> records = arrayBlockingQueue.poll(
                        concurrentTransformerConfig.getQueueFetchPollTimeMillis(), TimeUnit.MILLISECONDS
                );
                if (records == null) {
                    continue;
                }
                for (I record: records ) {
                    transformer.transform(chain, record);
                }
                recordOut += records.size();
                completed = (recordIn == recordOut) && records.isEmpty();
                records.clear();
            }
            operationCompleted = true;
            LOG.info("All data transfer completed.....");

            onCompleteHandler();
            LOG.info("End signal completed.....");
        } catch (InterruptedException ex) {
            this.exception = ex;
            LOG.warn("Error occurred", ex);
        } finally {
            operationCompleted = true;
        }
    }

    public void init() {

        LOG.info("Starting the concurrent");
        arrayBlockingQueue = new ArrayBlockingQueue<>(concurrentTransformerConfig.getQueueBufferSize());
        runningThread = new Thread(this);
        runningThread.setName(Thread.currentThread().getName() + "-child-" + chain.getName());
        runningThread.setDaemon(true);
        runningThread.start();
        LOG.info("Started the thread");
    }

    public void transfer(I input) {

        if (recordInCounter == 0) {
            tryBeginOnRecordArrival();
        }

        if (eventBatch.size() == concurrentTransformerConfig.getEventBatchSize()) {
            transferBatch(false);
            eventBatch = new ArrayList<>(concurrentTransformerConfig.getEventBatchSize());
        }
        eventBatch.add(input);
    }

    private void tryBeginOnRecordArrival() {
        if (!waitMonitorBegin.isNotified()) {
            onBegin(false);
        }
    }

    private void transferBatch(boolean spitEmptyList) {

        if (eventBatch.isEmpty() && !spitEmptyList) {
            return;
        }

        int sizeToBeTransferred = eventBatch.size();

        try {
            boolean isOffered;
            int countAttempt = concurrentTransformerConfig.getCountForInsertAttempt();
            do {
                isOffered = arrayBlockingQueue.offer(eventBatch, concurrentTransformerConfig.getQueueFetchPollTimeMillis(), TimeUnit.MILLISECONDS);
                countAttempt--;
            } while (!isOffered && countAttempt > 0);

            if (!isOffered) {
                throw new IllegalStateException("Could not push the data in specified time.");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        recordInCounter += sizeToBeTransferred;
    }

    private void onBeginHandler() throws InterruptedException {
        synchronized (waitMonitorBegin) {
            LOG.info("Start handler acquired lock");
            waitMonitorBegin.wait();
            if (waitMonitorBegin.isSignalIssued()) {
                transformer.onBegin(chain);
            }
            LOG.info("Start handler finish begin");
        }
    }

    public void onBegin(boolean isBeingEvent) {

        synchronized (waitMonitorBegin) {
            if (!waitMonitorBegin.isNotified()) {
                LOG.info("Start handler acquired lock.");
                if (isBeingEvent) {
                    waitMonitorBegin.setSignalIssued();
                }
                waitMonitorBegin.setNotified();
                waitMonitorBegin.notify();
                LOG.info("Start handler finish notification.");
            }
        }
    }

    private void onCompleteHandler() throws InterruptedException {
        synchronized (waitMonitorComplete) {
            LOG.info("Start wait.");
            waitMonitorComplete.wait();
            if (waitMonitorComplete.isSignalIssued()) {
                transformer.onComplete(chain);
            }
            LOG.info("End wait.");
        }
    }

    public void onComplete() {
        finish(true);
    }

    public void finish(boolean isCompleteCalled) {

        if (waitMonitorBegin.isNotified() && !isCompleteCalled) {
            // In case of there is not explicit begin call and there is no
            // event in this then, we should just make sure the start is
            // called to reach at end.
            onBegin(false);
        }

        transferBatch(false);
        this.recordIn = recordInCounter;

        synchronized (waitMonitorComplete) {
            transferBatch(true);
            if (!waitMonitorComplete.isNotified()) {
                LOG.info("Start notification.");
                if (isCompleteCalled) {
                    waitMonitorComplete.setSignalIssued();
                }
                waitMonitorComplete.setNotified();
                waitMonitorComplete.notify();

                LOG.info("End notification.");
            }
        }
    }

    public Exception getException() {
        return exception;
    }

    public boolean isStarted() {
        return isStarted;
    }

    public boolean isOperationCompleted() {
        return operationCompleted;
    }

    public long getRecordOut() {
        return recordOut;
    }

    public long getRecordIn() {
        return recordIn;
    }

    public long getRecordInCounter() {
        return recordInCounter;
    }

    public Thread.State threadState () {
        if (runningThread != null) {
            return null;
        }
        return runningThread.getState();
    }
}

class WaitMonitor {
    private boolean signalIssued = false;
    private boolean isNotified = false;

    public synchronized boolean isSignalIssued() {
        return signalIssued;
    }

    public synchronized void setSignalIssued() {
        this.signalIssued = true;
    }

    public synchronized boolean isNotified() {
        return isNotified;
    }

    public synchronized void setNotified() {
        isNotified = true;
    }
}
