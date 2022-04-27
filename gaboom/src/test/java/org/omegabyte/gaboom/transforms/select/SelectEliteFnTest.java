package org.omegabyte.gaboom.transforms.select;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SelectEliteFnTest {

    @Test
    @DisplayName("It should return the first n individuals from the input")
    public void testSingleOutput() throws Exception {

        DoFnTester<KV<String, SelectIndividuals<Integer>>, KV<String, Individuals<Integer>>> fnTester = DoFnTester.of(new SelectEliteFn<>());

        Individual<Integer> individual;
        List<Individual<Integer>> individualsList = new ArrayList<>();
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

        KV<String, SelectIndividuals<Integer>> testInput = KV.of("test", new SelectIndividuals<>(new Individuals<>(20, individualsList), 2));
        List<KV<String, Individuals<Integer>>> result = fnTester.processBundle(testInput);
        assert result.size() == 1;
        assertEquals("test", result.get(0).getKey());

        Individuals<Integer> individuals = result.get(0).getValue();
        assert individuals.getSeed() == 20;
        assertArrayEquals(individualsList.subList(0, 2).toArray(), individuals.getIndividuals().toArray());
    }

    @Test
    @DisplayName("It should return the first n individuals and their indices from the input")
    public void testMultiOutput() throws Exception {
        TupleTag<KV<String, List<Integer>>> selectedIndexesTT = new TupleTag<>();
        SelectEliteFn<Integer> selectEliteFn = new SelectEliteFn<>();
        selectEliteFn.setSelectIndicesTupleTag(selectedIndexesTT);
        DoFnTester<KV<String, SelectIndividuals<Integer>>, KV<String, Individuals<Integer>>> fnTester = DoFnTester.of(selectEliteFn);

        Individual<Integer> individual;
        List<Individual<Integer>> individualsList = new ArrayList<>();
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

        KV<String, SelectIndividuals<Integer>> testInput = KV.of("test", new SelectIndividuals<>(new Individuals<>(20, individualsList), 2));
        List<KV<String, Individuals<Integer>>> result = fnTester.processBundle(testInput);
        assert result.size() == 1;
        assertEquals("test", result.get(0).getKey());

        Individuals<Integer> individuals = result.get(0).getValue();
        assert individuals.getSeed() == 20;
        assertArrayEquals(individualsList.subList(0, 2).toArray(), individuals.getIndividuals().toArray());

        List<KV<String, List<Integer>>> resultIndices = fnTester.takeOutputElements(selectedIndexesTT);
        assert result.size() == 1;
        assertEquals("test", resultIndices.get(0).getKey());

        List<Integer> indexes = resultIndices.get(0).getValue();
        assertArrayEquals(indexes.toArray(), new Integer[]{0,1});
    }

    @Test
    @DisplayName("It should not return anything if n is larger than the number of provided individuals")
    public void testNTooBig() throws Exception {
        DoFnTester<KV<String, SelectIndividuals<Integer>>, KV<String, Individuals<Integer>>> fnTester = DoFnTester.of(new SelectEliteFn<>());

        Individual<Integer> individual;
        List<Individual<Integer>> individualsList = new ArrayList<>();
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

        KV<String, SelectIndividuals<Integer>> testInput = KV.of("test", new SelectIndividuals<>(new Individuals<>(20, individualsList), 10));
        List<KV<String, Individuals<Integer>>> result = fnTester.processBundle(testInput);
        assert result.isEmpty();
    }
}