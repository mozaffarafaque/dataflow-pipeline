package com.mozafaq.dataflow.pipeline;

/**
 * @author Mozaffar Afaque
 */
public class DelayConfig {

    private long delayBefore1;
    private long delayAfter1;
    private long delayBefore2;
    private long delayAfter2;

    public DelayConfig(long delayBefore1, long delayAfter1, long delayBefore2, long delayAfter2) {
        this.delayBefore1 = delayBefore1;
        this.delayAfter1 = delayAfter1;
        this.delayBefore2 = delayBefore2;
        this.delayAfter2 = delayAfter2;
    }

    public long getDelayBefore1() {
        return delayBefore1;
    }

    public long getDelayAfter1() {
        return delayAfter1;
    }

    public long getDelayBefore2() {
        return delayBefore2;
    }

    public long getDelayAfter2() {
        return delayAfter2;
    }

    @Override
    public String toString() {
        return "DelayConfig{" +
                "" + delayBefore1 +
                ", " + delayAfter1 +
                ", " + delayBefore2 +
                ", " + delayAfter2 +
                '}';
    }
}
