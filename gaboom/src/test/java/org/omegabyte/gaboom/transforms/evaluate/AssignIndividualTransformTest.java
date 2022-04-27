package org.omegabyte.gaboom.transforms.evaluate;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class AssignIndividualTransformTest {

    final static Random random = new Random();
    public static KV<String, Individual<Integer>> makeIndividual() {
        Individual<Integer> individual = new Individual<>(random, random.nextInt());
        return KV.of(individual.getId(), individual);
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testAssignIndividualTransform() {
        List<KV<String, Individual<Integer>>> individualList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            individualList.add(makeIndividual());
        }
        PCollection<KV<String, Individual<Integer>>> individualPCollection = pipeline.apply("Individual", Create.of(individualList));

        List<KV<String, String>> indices = new ArrayList<>();
        individualList.forEach(i -> {
            indices.add(KV.of(i.getKey(), "test"));
        });
        PCollection<KV<String, String>> indicesPCollection = pipeline.apply("Indices", Create.of(indices));

        PCollection<KV<String, Individual<Integer>>> output = individualPCollection.apply(AssignIndividualTransform.of(indicesPCollection));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individual<Integer>>>, Void>) kvs -> {
            Set<String> result = new HashSet<>();
            kvs.forEach(kv -> {
                assertEquals("test", kv.getKey());
                result.add(kv.getValue().getId());
            });

            Set<String> expected = new HashSet<>();
            individualList.forEach(i-> {
                expected.add(i.getKey());
            });
            assertEquals(expected, result);
            return null;
        });
        pipeline.run();
    }
}