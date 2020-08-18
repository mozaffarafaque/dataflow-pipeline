package com.mozafaq.dataflow.pipeline;

/**
 * @author Mozaffar Afaque
 */
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
