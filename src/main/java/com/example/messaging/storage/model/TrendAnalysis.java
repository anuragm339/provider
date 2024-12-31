package com.example.messaging.storage.model;

public class TrendAnalysis {
    private final double currentAverage;
    private final double previousAverage;
    private final double changePercent;
    private final TrendDirection direction;

    public enum TrendDirection {
        INCREASING,
        DECREASING,
        STABLE
    }

    public TrendAnalysis(double currentAverage, double previousAverage, double changePercent) {
        this.currentAverage = currentAverage;
        this.previousAverage = previousAverage;
        this.changePercent = changePercent;
        this.direction = calculateDirection(changePercent);
    }

    private TrendDirection calculateDirection(double changePercent) {
        if (Math.abs(changePercent) < 1.0) {
            return TrendDirection.STABLE;
        }
        return changePercent > 0 ? TrendDirection.INCREASING : TrendDirection.DECREASING;
    }

    public double getCurrentAverage() {
        return currentAverage;
    }

    public double getPreviousAverage() {
        return previousAverage;
    }

    public double getChangePercent() {
        return changePercent;
    }

    public TrendDirection getDirection() {
        return direction;
    }
}
