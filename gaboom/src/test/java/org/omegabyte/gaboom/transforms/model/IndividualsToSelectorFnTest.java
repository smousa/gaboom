package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;
import org.omegabyte.gaboom.CrossoverIndividuals;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class IndividualsToSelectorFnTest {

    @Test
    // It should convert to selector individuals
    public void testIndividualsToSelectorFn() throws Exception {
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

        List<KV<String, SelectIndividuals<Integer>>> expected = new ArrayList<>();
        expected.add(KV.of("test", new SelectIndividuals<>(20, individualsList, 1)));

        DoFnTester<KV<String, Individuals<Integer>>, KV<String, SelectIndividuals<Integer>>> fnTester = DoFnTester.of(new IndividualsToSelectorFn<>(1));
        List<KV<String, SelectIndividuals<Integer>>> actual = fnTester.processBundle(input);
        assert actual.size() == 1;
        Assert.assertEquals(expected.get(0).getKey(), actual.get(0).getKey());
        Assert.assertEquals(expected.get(0).getValue().getIndividuals(), actual.get(0).getValue().getIndividuals());
        Assert.assertEquals(expected.get(0).getValue().getN(), actual.get(0).getValue().getN());
        Assert.assertEquals(expected.get(0).getValue().getSeed(), actual.get(0).getValue().getSeed());
    }

    @Test
    // It should set the selector size to length if n is not set
    public void testAllIndividualsToSelectorFn() throws Exception {
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

        List<KV<String, SelectIndividuals<Integer>>> expected = new ArrayList<>();
        expected.add(KV.of("test", new SelectIndividuals<>(20, individualsList, 1)));

        DoFnTester<KV<String, Individuals<Integer>>, KV<String, SelectIndividuals<Integer>>> fnTester = DoFnTester.of(new IndividualsToSelectorFn<>());
        List<KV<String, SelectIndividuals<Integer>>> actual = fnTester.processBundle(input);
        assert actual.size() == 1;
        Assert.assertEquals(expected.get(0).getKey(), actual.get(0).getKey());
        Assert.assertEquals(expected.get(0).getValue().getIndividuals(), actual.get(0).getValue().getIndividuals());
        Assert.assertEquals(2, actual.get(0).getValue().getN());
        Assert.assertEquals(expected.get(0).getValue().getSeed(), actual.get(0).getValue().getSeed());

    }

}