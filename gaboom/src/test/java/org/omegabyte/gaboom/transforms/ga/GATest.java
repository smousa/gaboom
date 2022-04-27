package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Population;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Populate;
import org.omegabyte.gaboom.transforms.model.ModelGenerational;
import org.omegabyte.gaboom.transforms.model.ModelTest;
import org.omegabyte.gaboom.transforms.model.ModelTransform;
import org.omegabyte.gaboom.transforms.select.SelectTournamentFn;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GATest {
    private static final int seed = 0;
    private static final int popSize = 5;
    private static final int numPops = 2;
    private static final int numBest = 3;

    private static final Populate.PopulateTransform<String> populateTransform = Populate.as(new ModelTest.PopulateFn()).withPopSize(popSize);
    private static final Evaluate.EvaluateTransform<String> evaluateTransform = Evaluate.as(new ModelTest.FitnessTransform());
    private static final ModelTransform<String> modelTransform = new ModelGenerational<>(new SelectTournamentFn<>(3), Crossover.as(new ModelTest.CrossoverFn()).withCrossRate(0.7), Mutate.as(new ModelTest.MutateFn(3)).withMutRate(0.5));

    private static final TupleTag<Population<String>> populationTT = new TupleTag<>();
    private static final TupleTag<List<Individual<String>>> hallOfFameTT = new TupleTag<>();

    private static final Initialize<String> initialize = new Initialize<>(populateTransform, evaluateTransform, populationTT, hallOfFameTT, seed, numPops, numBest);
    private static final Evolve<String> evolve = new Evolve<>(modelTransform, evaluateTransform, populationTT, hallOfFameTT, numBest);

    public GATest() {
        ModelTest.SECRET_WORD = "redacted";
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testInitialize() {
        PCollectionTuple output = pipeline.apply(initialize);
        PAssert.that(output.get(populationTT)).satisfies((SerializableFunction<Iterable<Population<String>>, Void>) populations -> {
            Map<String, Population<String>> populationMap = new HashMap<>();
            populations.forEach(p -> populationMap.put(p.getId(), p));
            assertEquals(numPops, populationMap.size());

            Population<String> population;
            Individual<String> individual;

            assertTrue(populationMap.containsKey("JHg"));
            population = populationMap.get("JHg");
            assertEquals(0, population.getGenerations());
            assertEquals(popSize, population.getIndividuals().size());
            assertEquals(8967846198397165489L, population.getSeed());
            individual = population.getIndividuals().get(0);
            assertEquals("CyMxQ6", individual.getId());
            assertEquals("ofufbkgg", individual.getGenome());
            assertEquals(41.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(1);
            assertEquals("XXp3Lt", individual.getId());
            assertEquals("rfkcgkmp", individual.getGenome());
            assertEquals(43.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(2);
            assertEquals("VzQWbD", individual.getId());
            assertEquals("khveivbt", individual.getGenome());
            assertEquals(59.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(3);
            assertEquals("LxkoZu", individual.getId());
            assertEquals("wzlwguja", individual.getGenome());
            assertEquals(69.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(4);
            assertEquals("Aig2kO", individual.getId());
            assertEquals("cyqjosht", individual.getGenome());
            assertEquals(89.0, individual.getFitness(), 0);

            assertTrue(populationMap.containsKey("yxs"));
            population = populationMap.get("yxs");
            assertEquals(0, population.getGenerations());
            assertEquals(popSize, population.getIndividuals().size());
            assertEquals(4852331243713039087L, population.getSeed());
            individual = population.getIndividuals().get(0);
            assertEquals("swHwwb", individual.getId());
            assertEquals("iehrxved", individual.getGenome());
            assertEquals(53.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(1);
            assertEquals("NFKda9", individual.getId());
            assertEquals("nouijxkx", individual.getGenome());
            assertEquals(76.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(2);
            assertEquals("Qw14yk", individual.getId());
            assertEquals("buoulzwd", individual.getGenome());
            assertEquals(96.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(3);
            assertEquals("l1EoDO", individual.getId());
            assertEquals("kzjjymqw", individual.getGenome());
            assertEquals(103.0, individual.getFitness(), 0);
            individual = population.getIndividuals().get(4);
            assertEquals("N9EvEl", individual.getId());
            assertEquals("aynovsvo", individual.getGenome());
            assertEquals(109.0, individual.getFitness(), 0);

            return null;
        });
        PAssert.that(output.get(hallOfFameTT)).satisfies((SerializableFunction<Iterable<List<Individual<String>>>, Void>) lists -> {
            Iterator<List<Individual<String>>> iter = lists.iterator();
            List<Individual<String>> hallOfFame = iter.next();
            assertEquals(numBest, hallOfFame.size());

            Individual<String> individual;
            individual = hallOfFame.get(0);
            assertEquals("CyMxQ6", individual.getId());
            assertEquals("ofufbkgg", individual.getGenome());
            assertEquals(41.0, individual.getFitness(), 0);

            individual = hallOfFame.get(1);
            assertEquals("XXp3Lt", individual.getId());
            assertEquals("rfkcgkmp", individual.getGenome());
            assertEquals(43.0, individual.getFitness(), 0);

            individual = hallOfFame.get(2);
            assertEquals("swHwwb", individual.getId());
            assertEquals("iehrxved", individual.getGenome());
            assertEquals(53.0, individual.getFitness(), 0);

            assertFalse(iter.hasNext());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEvolve() {
        PCollectionTuple output = pipeline.apply(initialize).apply(evolve);
        PAssert.that(output.get(populationTT)).satisfies((SerializableFunction<Iterable<Population<String>>, Void>) populations -> {
            Map<String, Population<String>> populationMap = new HashMap<>();
            populations.forEach(p -> populationMap.put(p.getId(), p));
            assertEquals(numPops, populationMap.size());

            Population<String> population;
            Individual<String> individual;

            assertTrue(populationMap.containsKey("JHg"));
            population = populationMap.get("JHg");
            assertEquals(1, population.getGenerations());
            assertEquals(popSize, population.getIndividuals().size());
            assertEquals(2278088788093335886L, population.getSeed());

            assertTrue(populationMap.containsKey("yxs"));
            population = populationMap.get("yxs");
            assertEquals(1, population.getGenerations());
            assertEquals(popSize, population.getIndividuals().size());
            assertEquals(1835189412671696170L, population.getSeed());
            return null;
        });
        PAssert.that(output.get(hallOfFameTT)).satisfies((SerializableFunction<Iterable<List<Individual<String>>>, Void>) lists -> {
            Iterator<List<Individual<String>>> iter = lists.iterator();
            List<Individual<String>> hallOfFame = iter.next();
            assertEquals(numBest, hallOfFame.size());
            assertFalse(iter.hasNext());
            return null;
        });
        pipeline.run();
    }

}