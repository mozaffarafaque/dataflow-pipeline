package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * @author Mozaffar Afaque
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */

class ParallelOperationController<I, O> implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelOperationController.class);

    // For handling the data transfer and possible exceptions.
    private ArrayBlockingQueue<List<I>> arrayBlockingQueue;
    private Exception exception;
    private transient Thread runningThread;

    // Data pipeline related ones
    final private Transformer<I, O> transformer;
    final private PipelineChain<O> chain;
    final private ParallelOperationConfig parallelOperationConfig;

    private List<I> eventBatch;

    // Supplementary data
    private boolean isStarted;
    private boolean operationCompleted;
    private long recordOut;
    private long recordInCounter;

    final private Semaphore finishSemaphore;
    final private Semaphore beginSemaphore;


    // Monitors
    private final WaitMonitor waitMonitorBegin;
    private final WaitMonitor waitMonitorComplete;

    public ParallelOperationController(Transformer<I, O> transformer, PipelineChain<O> chain, ParallelOperationConfig parallelOperationConfig) {
        this.transformer = transformer;
        this.chain = chain;
        this.parallelOperationConfig = parallelOperationConfig;

        this.isStarted = false;
        this.operationCompleted = false;
        this.recordOut = 0;
        this.recordInCounter = 0;

        this.waitMonitorBegin = new WaitMonitor();
        this.waitMonitorComplete = new WaitMonitor();

        this.finishSemaphore = new Semaphore(1);
        this.beginSemaphore = new Semaphore(1);

        this.eventBatch = new ArrayList<>(parallelOperationConfig.getEventBatchSize());
    }

    @Override
    public synchronized void run() {

        LOG.info("Thread running....");
        isStarted = true;
        boolean completed = false;
        try {
            onBeginHandler();
            LOG.info("Begin signal received....");

            while (!completed) {

                List<I> records = arrayBlockingQueue.poll(
                        parallelOperationConfig.getQueuePollDuration().toMillis(), TimeUnit.MILLISECONDS
                );
                if (records == null) {
                    continue;
                }
                for (I record: records ) {
                    transformer.transform(chain, record);
                }
                recordOut += records.size();
                completed = records.isEmpty();
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
        arrayBlockingQueue = new ArrayBlockingQueue<>(parallelOperationConfig.getQueueBufferSize());
        runningThread = new Thread(this);
        runningThread.setName(Thread.currentThread().getName() + "-child-" + chain.getName());
        runningThread.setDaemon(true);

        try {
            finishSemaphore.acquire();
            beginSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        runningThread.start();
        LOG.info("Started the thread");
    }

    public void transfer(I input) {

        if (recordInCounter == 0) {
            onBegin(false);
        }

        if (eventBatch.size() == parallelOperationConfig.getEventBatchSize()) {
            transferBatch(false);
            eventBatch = new ArrayList<>(parallelOperationConfig.getEventBatchSize());
        }
        eventBatch.add(input);
    }


    private void transferBatch(boolean spitEmptyList) {

        if (eventBatch.isEmpty() && !spitEmptyList) {
            return;
        }

        int sizeToBeTransferred = eventBatch.size();

        try {
            boolean isOffered;
            int countAttempt = parallelOperationConfig.getCountForInsertAttempt();
            do {
                isOffered = arrayBlockingQueue.offer(eventBatch, parallelOperationConfig.getQueuePollDuration().toMillis(), TimeUnit.MILLISECONDS);
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
        beginSemaphore.acquire();
        if (waitMonitorBegin.isSignalIssued()) {
            transformer.onBegin(chain);
        }
    }

    public void onBegin(boolean isBeginEvent) {
        if (isBeginEvent) {
            waitMonitorBegin.setSignalIssued();
        }

        if (!waitMonitorBegin.isNotified()) {
            beginSemaphore.release();
            waitMonitorBegin.setNotified();
        }
    }

    private void onCompleteHandler()  {

        if (waitMonitorComplete.isSignalIssued()) {
            transformer.onComplete(chain);
        }

        finishSemaphore.release();
    }

    public void onComplete() {
        transferBatch(false);
        waitMonitorComplete.setSignalIssued();
        transferBatch(true);
    }

    public void finish(boolean isCompleteCalled) {
        LOG.info("Checking if waiting was not issued at all..");
        if (!waitMonitorBegin.isNotified()) {
            LOG.info("During finish - there was no begin ans no event to process. " +
                    "Calling is begin for process to flow");

            // In case of there is not explicit begin call and there is no
            // event in this then, we should just make sure the start is
            // called to reach at end.
            onBegin(false);
        }

        LOG.info("Transferring the remaining data if any..");

        transferBatch(false);

        LOG.info("Transferring empty batch for finish execution");

        transferBatch(true);

        try {
            LOG.info("Acquiring two subsequent execution....");
            finishSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
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

