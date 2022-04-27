package org.omegabyte.gaboom.transforms.halloffame;

import org.junit.Test;
import org.omegabyte.gaboom.Individual;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class HallOfFameFnTest {

    public static final Random random = new Random();

    public static Individual<Integer> makeIndividual(Double fitness) {
        Individual i = new Individual<>(random, random.nextInt());
        i.setFitness(fitness);
        return i;
    }

    @Test
    public void testCreateAccumulator() {
        HallOfFameFn fn = new HallOfFameFn(5);
        assertEquals(Collections.emptyList(), fn.createAccumulator());
    }

    @Test
    public void testAddInput() {
        List<Individual<Integer>> a = new ArrayList<>();
        a.add(makeIndividual(0.2));
        a.add(makeIndividual(0.3));
        a.add(makeIndividual(0.4));

        List<Individual<Integer>> b = new ArrayList<>();
        b.add(makeIndividual(0.25));
        b.add(makeIndividual(0.26));
        b.add(makeIndividual(0.45));
        b.add(makeIndividual(0.52));

        // b list is longer
        HallOfFameFn<Integer> fn = new HallOfFameFn<>(10);
        List<Double> actual = new ArrayList<>();
        fn.addInput(a, b).forEach(i -> actual.add(i.getFitness()));
        List<Double> expected = Arrays.asList(0.2, 0.25, 0.26, 0.3, 0.4, 0.45, 0.52);
        assertEquals(expected, actual);

        // a list is longer
        List<Double> actual2 = new ArrayList<>();
        fn.addInput(b, a).forEach(i -> actual2.add(i.getFitness()));
        assertEquals(expected, actual2);
    }

    @Test
    public void testMergeAccumulators() {
        List<Individual<Integer>> a = new ArrayList<>();
        a.add(makeIndividual(0.2));
        a.add(makeIndividual(0.3));
        a.add(makeIndividual(0.4));

        List<Individual<Integer>> b = new ArrayList<>();
        b.add(makeIndividual(0.25));
        b.add(makeIndividual(0.26));
        b.add(makeIndividual(0.45));
        b.add(makeIndividual(0.52));

        HallOfFameFn<Integer> fn = new HallOfFameFn<>(5);
        List<Double> actual = new ArrayList<>();
        fn.mergeAccumulators(Arrays.asList(a, b)).forEach(i -> actual.add(i.getFitness()));
        List<Double> expected = Arrays.asList(0.2, 0.25, 0.26, 0.3, 0.4);
        assertEquals(expected, actual);
    }

    @Test
    public void testExtractOutput() {
        List<Individual<Integer>> a = new ArrayList<>();
        a.add(makeIndividual(0.2));
        a.add(makeIndividual(0.3));
        a.add(makeIndividual(0.4));
        HallOfFameFn<Integer> fn = new HallOfFameFn<>(5);
        assertEquals(a, fn.extractOutput(a));
    }
}