package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CrossoverTest {

    static class SimpleCrossoverFn extends Crossover.CrossoverFn<Integer> implements Serializable {
        @Override
        public Integer crossover(ProcessContext context, Random random, Integer p1, Integer p2) {
            if (random.nextBoolean()) {
                return p1;
            } else {
                return p2;
            }
        }
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    // It should crossover a population based on its crossover rate
    public void testCrossoverTransform() {
        Individuals<Integer> individuals1 = new Individuals<>(0);
        Individual<Integer> individual;
        individual = new Individual<>("a", 1);
        individual.setFitness(0.1);
        individuals1.getIndividuals().add(individual);
        individual = new Individual<>("b", 2);
        individual.setFitness(0.3);
        individuals1.getIndividuals().add(individual);

        Individuals<Integer> individuals2 = new Individuals<>(1);
        individual = new Individual<>("c", 3);
        individual.setFitness(0.4);
        individuals2.getIndividuals().add(individual);
        individual = new Individual<>("d", 4);
        individual.setFitness(0.6);
        individuals2.getIndividuals().add(individual);

        Individuals<Integer> individuals3 = new Individuals<>(2);
        individual = new Individual<>("e", 5);
        individual.setFitness(0.6);
        individuals3.getIndividuals().add(individual);
        individual = new Individual<>("f", 6);
        individual.setFitness(0.85);
        individuals3.getIndividuals().add(individual);


        Crossover.CrossoverFn<Integer> crossoverFn = new SimpleCrossoverFn();
        PCollectionView<Integer> sideInput = pipeline.apply("SideInput", Create.of(4))
                .apply(View.asSingleton());

        PCollection<KV<String, Individuals<Integer>>> population = pipeline
                .apply(Create.of(
                        KV.of("test1", individuals1),
                        KV.of("test2", individuals2),
                        KV.of("test3", individuals3)))
                .apply(Crossover.as(crossoverFn).withCrossRate(.5).withSideInputs(sideInput));

        PAssert.that(population).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) output -> {
            KV<String, Individuals<Integer>> result = output.iterator().next();
            Map<String, Individuals<Integer>> resultMap = new HashMap<>();
            output.forEach(kv-> resultMap.put(kv.getKey(), kv.getValue()));

            assertEquals(-4962768465676381896L, resultMap.get("test1").getSeed());
            assertEquals("7157Kh", resultMap.get("test1").getIndividuals().get(0).getId());
            assertEquals(individuals1.getIndividuals().get(0).getGenome(), resultMap.get("test1").getIndividuals().get(0).getGenome());
            assertNull(resultMap.get("test1").getIndividuals().get(0).getFitness());
            assertEquals("r8Ry8R", resultMap.get("test1").getIndividuals().get(1).getId());
            assertEquals(individuals1.getIndividuals().get(1).getGenome(), resultMap.get("test1").getIndividuals().get(1).getGenome());
            assertNull(resultMap.get("test1").getIndividuals().get(1).getFitness());

            assertEquals(-4964420948893066024L, resultMap.get("test2").getSeed());
            assertEquals("64iE89", resultMap.get("test2").getIndividuals().get(0).getId());
            assertEquals(individuals2.getIndividuals().get(1).getGenome(), resultMap.get("test2").getIndividuals().get(0).getGenome());
            assertNull(resultMap.get("test1").getIndividuals().get(0).getFitness());
            assertEquals("xrGe8I", resultMap.get("test2").getIndividuals().get(1).getId());
            assertEquals(individuals2.getIndividuals().get(1).getGenome(), resultMap.get("test2").getIndividuals().get(1).getGenome());
            assertNull(resultMap.get("test1").getIndividuals().get(1).getFitness());

            assertEquals(-4959463499243013640L, resultMap.get("test3").getSeed());
            assertEquals(individuals3.getIndividuals().get(0).getId(), resultMap.get("test3").getIndividuals().get(0).getId());
            assertEquals(individuals3.getIndividuals().get(0).getGenome(), resultMap.get("test3").getIndividuals().get(0).getGenome());
            assertEquals(individuals3.getIndividuals().get(0).getFitness(), resultMap.get("test3").getIndividuals().get(0).getFitness());
            assertEquals(individuals3.getIndividuals().get(1).getId(), resultMap.get("test3").getIndividuals().get(1).getId());
            assertEquals(individuals3.getIndividuals().get(1).getGenome(), resultMap.get("test3").getIndividuals().get(1).getGenome());
            assertEquals(individuals3.getIndividuals().get(1).getFitness(), resultMap.get("test3").getIndividuals().get(1).getFitness());
            return null;
        });
        pipeline.run();

    }
}