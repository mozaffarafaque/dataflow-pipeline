package com.mozafaq.dataflow.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
    private volatile Exception exception;
    private volatile Thread runningThread;

    // Data pipeline related ones
    final private Transformer<I, O> transformer;
    final private PipelineChain<O> chain;
    final private ParallelOperationConfig parallelOperationConfig;

    private volatile List<I> eventBatch;

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

        this.exception = null;
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
    public void run() {

        LOG.info(chain.getName() + "Thread running....");
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
                for (I record : records) {
                    transformer.transform(chain, record);
                }
                recordOut += records.size();
                LOG.info(chain.getName() + "-The record info received " + records);
                completed = records.isEmpty();
                records.clear();
            }
            LOG.info("All data transfer completed.....");

            onCompleteHandler();
            LOG.info("End signal completed.....");
        } catch (Exception ex) {
            this.exception = ex;
            LOG.warn("Error occurred", ex);
        } finally {
        }
    }

    public synchronized void init() {

        if (runningThread != null) {
            return;
        }
        LOG.info(chain.getName() + "-Initializing the concurrent controller.");
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
        LOG.info(chain.getName() + "-Completed the concurrent controller.");
    }

    public void transfer(I input) {

        if (recordInCounter == 0) {
            onBegin(false);
        }

        if (eventBatch.size() == parallelOperationConfig.getEventBatchSize()) {
            flushBuffer();
        }
        eventBatch.add(input);
    }

    private synchronized void flushBuffer() {
        List<I> existingEvents = eventBatch;
        eventBatch = new ArrayList<>(parallelOperationConfig.getEventBatchSize());
        transferBatch(false, existingEvents);
    }

    private void transferBatch(boolean spitEmptyList, List<I> events) {

        if (events.isEmpty() && !spitEmptyList) {
            return;
        }

        int sizeToBeTransferred = events.size();

        try {
            boolean isOffered;
            int countAttempt = parallelOperationConfig.getCountForInsertAttempt();
            do {
                failOnChildException();
                isOffered = arrayBlockingQueue.offer(events, parallelOperationConfig.getQueuePollDuration().toMillis(), TimeUnit.MILLISECONDS);
                countAttempt--;
            } while (!isOffered && countAttempt > 0);

            if (!isOffered) {
                throw new IllegalStateException(chain.getName() + "-Could not push the data in specified time.");
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

    public synchronized void onBegin(boolean isBeginEvent) {
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
        waitMonitorComplete.setCompleted();
        LOG.info(chain.getName() + "-After release " + arrayBlockingQueue.size());
    }

    public synchronized void onComplete() {
        LOG.info(chain.getName() + "-On complete execution start.. record In: " + recordInCounter +", array size " + arrayBlockingQueue.size());

        flushBuffer();
        LOG.info(chain.getName() + "-On complete: after transferring any remaining.. record In: " + recordInCounter +", array size " + arrayBlockingQueue.size());
        waitMonitorComplete.setSignalIssued();
        transferBatch(true, Collections.emptyList());
        LOG.info(chain.getName() + "-After complete if waiting was not issued at all.. record In: " + recordInCounter +", array size " + arrayBlockingQueue.size());
    }

    private void failOnChildException() {
        if (this.exception != null) {
            throw new IllegalStateException("[" + chain.getName() + "] Exception encountered.", this.exception);
        }
    }

    public synchronized void finish() {
        LOG.info(chain.getName() + "-Checking if waiting was not issued at all.. record In: " + recordInCounter);

        if (!waitMonitorBegin.isNotified()) {
            LOG.info(chain.getName() + " -During finish - there was no begin ans no event to process. " +
                    "Calling is begin for process to flow");

            // In case of there is not explicit begin call and there is no
            // event in this then, we should just make sure the start is
            // called to reach at end.
            onBegin(false);
        }

        LOG.info(chain.getName() + "-Transferring the remaining data if any..");

        flushBuffer();
        LOG.info(chain.getName() + "-Transferring empty batch for finish execution");

        transferBatch(true, Collections.emptyList());

        failOnChildException();
        try {
            LOG.info(chain.getName() + "-Acquiring subsequent execution....");
            finishSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
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

