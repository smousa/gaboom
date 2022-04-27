package org.omegabyte.gaboom.transforms.evaluate;

import org.junit.Before;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SortIndividualsFnTest {
    private static final long seed = 0;

    private static final Random random = new Random();

    public static Individual<Integer> makeIndividual() {
        Individual<Integer> individual = new Individual<Integer>(random, random.nextInt());
        individual.setFitness(random.nextDouble());
        return individual;
    }

    @Before
    public void setUpTest() {
        random.setSeed(seed);
    }

    @Test
    public void testCreateAccumulator() {
        assertEquals(Collections.emptyList(), new SortIndividualsFn().createAccumulator());
    }

    @Test
    public void testAddInput() {
        SortIndividualsFn<Integer> combineFn = new SortIndividualsFn<>();

        List<Individual<Integer>> result = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            result = combineFn.addInput(result, makeIndividual());
            Double previous = result.get(0).getFitness();
            for(Individual<Integer> ind: result.subList(1, result.size())) {
                assertTrue(previous <= ind.getFitness());
                previous = ind.getFitness();
            }
        }
    }

    @Test
    public void testAddInputSameWithSameFitness() {
        SortIndividualsFn<Integer> combineFn = new SortIndividualsFn<>();

        Individual<Integer> a = new Individual<Integer>("a", 1234);
        a.setFitness(1.0);
        Individual<Integer> b = new Individual<Integer>("b", 2345);
        b.setFitness(1.0);

        List<Individual<Integer>> actual;

        actual = new ArrayList<>();
        actual = combineFn.addInput(actual, a);
        actual = combineFn.addInput(actual, b);
        assertEquals(Arrays.asList(a, b), actual);

        actual = new ArrayList<>();
        actual = combineFn.addInput(actual, b);
        actual = combineFn.addInput(actual, a);
        assertEquals(Arrays.asList(a, b), actual);
    }

    @Test
    public void testMergeAccumulators() {
        SortIndividualsFn<Integer> combineFn = new SortIndividualsFn<>();

        List<Individual<Integer>> aList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            aList = combineFn.addInput(aList, makeIndividual());
        }

        List<Individual<Integer>> bList = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            bList = combineFn.addInput(bList, makeIndividual());
        }

        List<Individual<Integer>> result = combineFn.mergeAccumulators(Arrays.asList(aList, bList));
        Double previous = result.get(0).getFitness();
        for(Individual<Integer> ind: result.subList(1, result.size())) {
            assertTrue(previous <= ind.getFitness());
            previous = ind.getFitness();
        }

        result = combineFn.mergeAccumulators(Arrays.asList(bList, aList));
        previous = result.get(0).getFitness();
        for(Individual<Integer> ind: result.subList(1, result.size())) {
            assertTrue(previous <= ind.getFitness());
            previous = ind.getFitness();
        }
    }

    @Test
    public void testMergeAccumulatorsWithSameFitness() {
        SortIndividualsFn<Integer> combineFn = new SortIndividualsFn<>();

        Individual<Integer> a = new Individual<Integer>("a", 1234);
        a.setFitness(1.0);
        Individual<Integer> b = new Individual<Integer>("b", 2345);
        b.setFitness(1.0);

        List<Individual<Integer>> actual;

        actual = combineFn.mergeAccumulators(Arrays.asList(Arrays.asList(a), Arrays.asList(b)));
        assertEquals(Arrays.asList(a, b), actual);
        actual = combineFn.mergeAccumulators(Arrays.asList(Arrays.asList(b), Arrays.asList(a)));
        assertEquals(Arrays.asList(a, b), actual);
    }

    @Test
    public void testExtractOutput() {
        SortIndividualsFn<Integer> combineFn = new SortIndividualsFn<>();

        List<Individual<Integer>> aList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            aList = combineFn.addInput(aList, makeIndividual());
        }
        assertEquals(aList, combineFn.extractOutput(aList));
    }
}