package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.ArrayList;
import java.util.List;

public class GetIndividualListFnTest {

    @Test
    // It should return the individual list from individuals
    public void testGetIndividualListFn() throws Exception {
        List<Individual<Integer>> individualsList = new ArrayList<>();
        Individual<Integer> individual;
        individual = new Individual<>("a", 1);
        individual.setFitness(0.1);
        individualsList.add(individual);
        individual = new Individual<>("b", 2);
        individual.setFitness(0.3);
        individualsList.add(individual);

        DoFnTester<KV<String, Individuals<Integer>>, List<Individual<Integer>>> fnTester = DoFnTester.of(new GetIndividualListFn<>());
        Assert.assertEquals(individualsList, fnTester.processBundle(KV.of("test", new Individuals<>(20, individualsList))).get(0));
    }

}