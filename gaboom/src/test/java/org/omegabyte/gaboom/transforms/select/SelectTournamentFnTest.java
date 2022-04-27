package org.omegabyte.gaboom.transforms.select;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Before;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SelectTournamentFnTest {
    private List<Individual<Integer>> individualsList;

    public static Individual[] arrayOfIndividuals(Individual ...individuals) {
        return individuals;
    }

    @Before
    public void initEach() {
        individualsList = new ArrayList<>();

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
        individual = new Individual<>("e", 5);
        individual.setFitness(2.7);
        individualsList.add(individual);
        individual = new Individual<>("f", 6);
        individual.setFitness(2.8);
        individualsList.add(individual);
    }

    @Test
    //@DisplayName("It should select n individuals")
    public void testProcessElement() throws Exception {
        TupleTag<KV<String, List<Integer>>> selectedIndexesTT = new TupleTag<>();
        SelectTournamentFn<Integer> selectTournamentFn = new SelectTournamentFn<>(3);
        selectTournamentFn.setSelectIndicesTupleTag(selectedIndexesTT);
        DoFnTester<KV<String, SelectIndividuals<Integer>>, KV<String, Individuals<Integer>>> fnTester = DoFnTester.of(selectTournamentFn);

        KV<String, SelectIndividuals<Integer>> testInput = KV.of("test", new SelectIndividuals<>(new Individuals<>(0, individualsList), 2));
        List<KV<String, Individuals<Integer>>> result = fnTester.processBundle(testInput);
        assert result.size() == 1;
        assertEquals("test", result.get(0).getKey());
        Individuals<Integer> individuals = result.get(0).getValue();
        assertEquals(-8292973307042192125L, individuals.getSeed());
        assertArrayEquals(arrayOfIndividuals(individualsList.get(0), individualsList.get(1)), individuals.getIndividuals().toArray());

        List<KV<String, List<Integer>>> resultIndices = fnTester.takeOutputElements(selectedIndexesTT);
        assert  resultIndices.size() == 1;
        assertEquals("test", resultIndices.get(0).getKey());
        List<Integer> indices = resultIndices.get(0).getValue();
        assertArrayEquals(new Integer[]{0,1}, indices.toArray());
    }

    @Test
    //@DisplayName("It should not return anything if there are not enough individuals")
    public void testNotEnough() throws Exception {
        DoFnTester<KV<String, SelectIndividuals<Integer>>, KV<String, Individuals<Integer>>> fnTester = DoFnTester.of(new SelectTournamentFn<>(5));
        KV<String, SelectIndividuals<Integer>> testInput = KV.of("test", new SelectIndividuals<>(new Individuals<>(20, individualsList), 10));
        assert fnTester.processBundle(testInput).isEmpty();
    }
}