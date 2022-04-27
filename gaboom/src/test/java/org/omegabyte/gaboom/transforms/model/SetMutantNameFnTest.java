package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SetMutantNameFnTest {
    private final Random random = new Random();

    @Test
    public void testSetMutantNameNoChange() throws Exception {
        Individual<Integer> individual = new Individual<>(random, 1234);
        individual.setFitness(1.0);
        Individuals<Integer> individuals = new Individuals<>(0);
        individuals.getIndividuals().add(individual);

        DoFnTester<KV<String, Individuals<Integer>>, KV<String, Individuals<Integer>>> fnTester = DoFnTester.of(new SetMutantNameFn<>());
        List<KV<String, Individuals<Integer>>> result = fnTester.processBundle(KV.of("test", individuals));
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getKey());
        assertEquals(-4962768465676381896L, result.get(0).getValue().getSeed());
        assertEquals(1, result.get(0).getValue().getIndividuals().size());

        Individual<Integer> actual = result.get(0).getValue().getIndividuals().get(0);
        assertEquals(individual.getId(), actual.getId());
        assertEquals(individual.getGenome(), actual.getGenome());
        assertEquals(individual.getFitness(), actual.getFitness());
    }

    @Test
    public void testSetMutantNameWithChange() throws Exception {
        Individual<Integer> individual = new Individual<>("abc123.dfg", 1234);
        individual.setFitness(1.0);
        Individuals<Integer> individuals = new Individuals<>(0);
        individuals.getIndividuals().add(individual);

        DoFnTester<KV<String, Individuals<Integer>>, KV<String, Individuals<Integer>>> fnTester = DoFnTester.of(new SetMutantNameFn<>());
        List<KV<String, Individuals<Integer>>> result = fnTester.processBundle(KV.of("test", individuals));
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getKey());
        assertEquals(-4962768465676381896L, result.get(0).getValue().getSeed());
        assertEquals(1, result.get(0).getValue().getIndividuals().size());

        Individual<Integer> actual = result.get(0).getValue().getIndividuals().get(0);
        assertEquals("Pbd715", actual.getId());
        assertEquals(individual.getGenome(), actual.getGenome());
        assertEquals(individual.getFitness(), actual.getFitness());

    }

}