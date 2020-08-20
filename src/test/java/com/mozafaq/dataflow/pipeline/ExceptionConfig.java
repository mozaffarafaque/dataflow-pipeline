package com.mozafaq.dataflow.pipeline;

import java.util.Set;

/**
 *
 *
 */
public class ExceptionConfig {
    private Set<Integer> beforeEventsInt;
    private Set<String> beforeEventsStr;

    private Set<Integer> laterEventsInt;
    private Set<String> laterEventsStr;

    private boolean exceptionOnBegin = false;
    private boolean exceptionOnComplete = false;

    public ExceptionConfig() {
    }

    public ExceptionConfig(Set<Integer> beforeEventsInt, Set<String> beforeEventsStr, Set<Integer> laterEventsInt, Set<String> laterEventsStr) {
        this.beforeEventsInt = beforeEventsInt;
        this.beforeEventsStr = beforeEventsStr;
        this.laterEventsInt = laterEventsInt;
        this.laterEventsStr = laterEventsStr;
    }

    public ExceptionConfig(boolean exceptionOnBegin, boolean exceptionOnComplete) {
        this.exceptionOnBegin = exceptionOnBegin;
        this.exceptionOnComplete = exceptionOnComplete;
    }

    public Set<Integer> getBeforeEventsInt() {
        return beforeEventsInt;
    }

    public Set<String> getBeforeEventsStr() {
        return beforeEventsStr;
    }

    public Set<Integer> getLaterEventsInt() {
        return laterEventsInt;
    }

    public Set<String> getLaterEventsStr() {
        return laterEventsStr;
    }

    public boolean isExceptionOnBegin() {
        return exceptionOnBegin;
    }

    public boolean isExceptionOnComplete() {
        return exceptionOnComplete;
    }

    @Override
    public String toString() {
        return "ExceptionConfig{" +
                "beforeEventsInt=" + beforeEventsInt +
                ", beforeEventsStr=" + beforeEventsStr +
                ", laterEventsInt=" + laterEventsInt +
                ", laterEventsStr=" + laterEventsStr +
                '}';
    }
}

