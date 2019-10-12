package component;

/**
 * accumulator for computing average across nodes
 *
 * @author Nosolution
 * @version 1.0
 * @since 2019/10/12
 */
public class AverageAccumulator implements AccumulatorV2<Double, Double> {
    private Long n = 0L;
    private Double average = 0.0;

    public void reset() {
        average = 0.0;
    }

    public void add(Double d) {
        average = (n * average + d) / (n + 1);
        n += 1;
    }

}
