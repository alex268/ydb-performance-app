package tech.ydb.performance.api;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class Metric {
    private final String name;
    private final double value;

    public Metric(String name, double value) {
        this.name = name;
        this.value = value;
    }

    public String name() {
        return name;
    }

    public double value() {
        return value;
    }
}
