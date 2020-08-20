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

    private final Logger LOG = LoggerFactory.getLogger(ParallelOperationController.class);

    // For handling the data transfer and possible exceptions.
    private ArrayBlockingQueue<List<I>> transferQueue;
    private volatile Exception exception;
    private volatile Thread runningThread;

    // Data pipeline related ones
    final private Transformer<I, O> transformer;
    final private PipelineChain<O> chain;
    final private ParallelOperationConfig operationConfig;

    private volatile List<I> eventBatch;

    // Supplementary data
    private long recordOut;
    private long recordInCounter;

    final private Semaphore finishSemaphore;
    final private Semaphore beginSemaphore;

    // Monitors
    private final WaitMonitor waitMonitorBegin;
    private final WaitMonitor waitMonitorComplete;

    public ParallelOperationController(Transformer<I, O> transformer, PipelineChain<O> chain, ParallelOperationConfig operationConfig) {
        this.transformer = transformer;
        this.chain = chain;
        this.operationConfig = operationConfig;

        this.exception = null;
        this.recordOut = 0;
        this.recordInCounter = 0;

        this.waitMonitorBegin = new WaitMonitor();
        this.waitMonitorComplete = new WaitMonitor();

        this.finishSemaphore = new Semaphore(1);
        this.beginSemaphore = new Semaphore(1);

        this.eventBatch = new ArrayList<>(operationConfig.getEventBatchSize());
    }

    @Override
    public void run() {

        LOG.info(chain.getName() + ": Thread running.");
        boolean completed = false;
        try {
            onBeginHandler();
            LOG.info(chain.getName() + ": Begin signal received.");

            while (!completed) {

                List<I> records = transferQueue.poll(
                        operationConfig.getQueuePollDuration().toMillis(), TimeUnit.MILLISECONDS
                );
                if (records == null) {
                    continue;
                }
                for (I record : records) {
                    transformer.transform(chain, record);
                }
                recordOut += records.size();
                completed = records.isEmpty();
                records.clear();
            }
            LOG.info(chain.getName() + ": All data transfer completed.");

            onCompleteHandler(true);
            LOG.info(chain.getName() + ": End signal completed.");
        } catch (Exception ex) {
            this.exception = ex;
            LOG.warn(chain.getName() + ": Error occurred", this.exception);
        } finally {
            onCompleteHandler(false);
        }
    }

    public synchronized void init() {

        if (runningThread != null) {
            return;
        }
        LOG.info(chain.getName() + ": Initializing the concurrent controller.");
        transferQueue = new ArrayBlockingQueue<>(operationConfig.getQueueBufferSize());
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
    }

    public void transfer(I input) {

        if (recordInCounter == 0) {
            onBegin(false);
        }

        if (eventBatch.size() == operationConfig.getEventBatchSize()) {
            flushBuffer();
        }
        eventBatch.add(input);
    }

    private synchronized void flushBuffer() {
        List<I> existingEvents = eventBatch;
        eventBatch = new ArrayList<>(operationConfig.getEventBatchSize());
        transferBatch(false, existingEvents);
    }

    private void transferBatch(boolean spitEmptyList, List<I> events) {

        if (events.isEmpty() && !spitEmptyList) {
            return;
        }

        int sizeToBeTransferred = events.size();

        try {
            boolean isOffered;
            int countAttempt = operationConfig.getCountForInsertAttempt();
            do {
                failOnChildException();
                isOffered = transferQueue.offer(events, operationConfig.getQueuePollDuration().toMillis(), TimeUnit.MILLISECONDS);
                countAttempt--;
            } while (!isOffered && countAttempt > 0);

            if (!isOffered) {
                throw new IllegalStateException(chain.getName() + ": Could not push the data in specified time.");
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

    private void onCompleteHandler(boolean completeCallEnabled)  {

        if (completeCallEnabled && waitMonitorComplete.isSignalIssued()) {
            transformer.onComplete(chain);
        }

        finishSemaphore.release();
        waitMonitorComplete.setCompleted();
    }

    public synchronized void onComplete() {

        flushBuffer();
        waitMonitorComplete.setSignalIssued();
        transferBatch(true, Collections.emptyList());
    }

    private void failOnChildException() {
        if (this.exception != null) {
            throw new IllegalStateException("[" + chain.getName() + "] Exception encountered.", this.exception);
        }
    }

    public synchronized void finish() {

        if (!waitMonitorBegin.isNotified()) {
            LOG.info(chain.getName() + ": During finish - there was no begin ans no event to process. " +
                    "Calling is begin for process to flow.");

            // In case of there is not explicit begin call and there is no
            // event in this then, we should just make sure the start is
            // called to reach at end.
            onBegin(false);
        }


        flushBuffer();

        transferBatch(true, Collections.emptyList());

        failOnChildException();
        try {
            LOG.info(chain.getName() + "-Acquiring subsequent execution.");
            boolean isAcquired = finishSemaphore.tryAcquire(operationConfig.getQueuePollDuration().toMillis() * operationConfig.getCountForInsertAttempt(), TimeUnit.MILLISECONDS);
            if (!isAcquired) {
                throw new IllegalStateException( chain.getName() + ": Child thread could not complete execution.");
            }

        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        failOnChildException();
    }

    public void killRunningParallelExecutions() {
        if (runningThread.getState() != Thread.State.TERMINATED) {
            LOG.warn(chain.getName() + ": Killing this thread because of child exception/timeout/termination", this.exception);
            runningThread.interrupt();
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

