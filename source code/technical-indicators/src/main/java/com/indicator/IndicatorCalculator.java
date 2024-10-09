package com.indicator;

import org.apache.kafka.streams.Topology;

public abstract class IndicatorCalculator {
    public abstract Topology createTopology();
}
