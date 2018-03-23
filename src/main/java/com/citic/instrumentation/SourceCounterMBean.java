package com.citic.instrumentation;

/**
 * This interface represents a table counter mbean. Any class implementing
 * this interface must sub-class
 * {@linkplain org.apache.flume.instrumentation.MonitoredCounterGroup}. This
 */
public interface SourceCounterMBean {
    String getReceivedTableCount();
}
