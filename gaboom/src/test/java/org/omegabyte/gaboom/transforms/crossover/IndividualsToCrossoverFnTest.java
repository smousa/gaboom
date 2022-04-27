package org.omegabyte.gaboom.transforms.crossover;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;
import org.omegabyte.gaboom.CrossoverIndividuals;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.ArrayList;
import java.util.List;

public class IndividualsToCrossoverFnTest {
    @Test
    // @DisplayName("It should not convert to crossover individuals if the size is not 2")
    public void testInvalidIndividuals() throws Exception {
        List<Individual<Integer>> individualsList = new ArrayList<>();
        Individual<Integer> individual;
        individual = new Individual<>("a", 1);
        individual.setFitness(0.1);
        individualsList.add(individual);
        individual = new Individual<>("b", 2);
        individual.setFitness(0.3);
        individualsList.add(individual);
        individual = new Individual<>("c", 3);
        individual.setFitness(0.8);
        individualsList.add(individual);
        individual = new Individual<>("d", 4);
        individual.setFitness(2.0);
        individualsList.add(individual);


        DoFnTester<KV<String, Individuals<Integer>>, KV<String, CrossoverIndividuals<Integer>>> fnTester = DoFnTester.of(new IndividualsToCrossoverFn<>());
        assert fnTester.processBundle(KV.of("test", new Individuals<>(20, individualsList))).isEmpty();
    }

    @Test
    // It should convert to crossover individuals if the size is 2
    public void testValidIndividuals() throws Exception {
        List<Individual<Integer>> individualsList = new ArrayList<>();
        Individual<Integer> individual;
        individual = new Individual<>("a", 1);
        individual.setFitness(0.1);
        individualsList.add(individual);
        individual = new Individual<>("b", 2);
        individual.setFitness(0.3);
        individualsList.add(individual);


        List<KV<String, Individuals<Integer>>> input = new ArrayList<>();
        input.add(KV.of("test", new Individuals<>(20, individualsList)));

        List<KV<String, CrossoverIndividuals<Integer>>> expected = new ArrayList<>();
        expected.add(KV.of("test", new CrossoverIndividuals<>(20, individualsList.get(0), individualsList.get(1))));

        DoFnTester<KV<String, Individuals<Integer>>, KV<String, CrossoverIndividuals<Integer>>> fnTester = DoFnTester.of(new IndividualsToCrossoverFn<>());
        List<KV<String, CrossoverIndividuals<Integer>>> actual = fnTester.processBundle(input);
        assert actual.size() == 1;
        Assert.assertEquals(expected.get(0).getKey(), actual.get(0).getKey());
        Assert.assertEquals(expected.get(0).getValue().getP1(), actual.get(0).getValue().getP1());
        Assert.assertEquals(expected.get(0).getValue().getP2(), actual.get(0).getValue().getP2());
        Assert.assertEquals(expected.get(0).getValue().getSeed(), actual.get(0).getValue().getSeed());
    }
}
