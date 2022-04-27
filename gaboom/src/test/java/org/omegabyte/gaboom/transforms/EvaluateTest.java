package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

public class EvaluateTest {
    public static final long seed = 0;
    public static final Random random = new Random();

    @Before
    public void setUpTest() {
        random.setSeed(seed);
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    public static Individuals<Integer> makeIndividuals(int size) {
        Individuals<Integer> individuals = new Individuals<>(random.nextLong());
        for (int i = 0; i < size; i++) {
            Individual<Integer> ind = new Individual<>(random, random.nextInt());
            if (random.nextBoolean()) {
                ind.setFitness(random.nextDouble());
            }
            individuals.getIndividuals().add(ind);
        }

        return individuals;
    }

    static class RandomFitnessTransform extends Evaluate.FitnessTransform<Integer> implements Serializable{
        static class RandomFitnessFn extends DoFn<Individual<Integer>, Individual<Integer>> {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Individual<Integer> ind = c.element();
                Individual<Integer> out = new Individual<>(ind.getId(), ind.getGenome());
                out.setFitness(random.nextDouble());
                c.output(out);
            }
        }

        @Override
        public PCollection<Individual<Integer>> expand(PCollection<Individual<Integer>> input) {
            return input.apply(ParDo.of(new RandomFitnessFn()));
        }
    }

    @Test
    public void testEvaluate() {
        Individuals<Integer> individuals = makeIndividuals(10);
        Evaluate.EvaluateTransform<Integer> transform = Evaluate.as(new RandomFitnessTransform());
        PCollection<KV<String, Individuals<Integer>>> output = pipeline
                .apply(Create.of(KV.of("test", individuals)))
                .apply(transform);
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) kvs -> {
            KV<String, Individuals<Integer>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(4804307197456638271L, result.getValue().getSeed());

            Map<String, Individual<Integer>> expected = new HashMap<>();
            individuals.getIndividuals().forEach(i-> expected.put(i.getId(), i));

            assertEquals(expected.size(), result.getValue().getIndividuals().size());

            Double previousValue = result.getValue().getIndividuals().get(0).getFitness();
            for(Individual<Integer> i: result.getValue().getIndividuals()) {
                assertTrue(expected.containsKey(i.getId()));

                Individual<Integer> exp = expected.get(i.getId());
                if (exp.getFitness() == null) {
                    assertTrue(i.getFitness() != null);
                } else {
                    assertEquals(exp.getFitness(), i.getFitness());
                }

                assertTrue(previousValue <= i.getFitness());
                previousValue = i.getFitness();
            };
            return null;
        });
        pipeline.run();
    }
}