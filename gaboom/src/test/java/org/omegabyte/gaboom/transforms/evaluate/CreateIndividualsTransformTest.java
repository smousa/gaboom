package org.omegabyte.gaboom.transforms.evaluate;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class CreateIndividualsTransformTest {

    final static Random random = new Random();
    public static KV<String, List<Individual<Integer>>> makeIndividuals(String key, int size) {
        List<Individual<Integer>> individuals = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            individuals.add(new Individual<Integer>(random, random.nextInt()));
        }
        return KV.of(key, individuals);
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCreateIndividualsTransform() {
        KV<String, List<Individual<Integer>>> individualList = makeIndividuals("test", 5);
        PCollection<KV<String, List<Individual<Integer>>>> individualPCollection = pipeline.apply("Individual", Create.of(individualList));
        PCollection<KV<String, BaseItem>> baseItemPCollection = pipeline.apply("BaseItem", Create.of(KV.of("test", new BaseItem(20))));
        PCollection<KV<String, Individuals<Integer>>> output = individualPCollection.apply(CreateIndividualsTransform.of(baseItemPCollection));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) kvs -> {
            KV<String, Individuals<Integer>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            List<String> actual = new ArrayList<>();
            result.getValue().getIndividuals().forEach(i-> {
                actual.add(i.getId());
            });

            List<String> expected = new ArrayList<>();
            individualList.getValue().forEach(i -> {
                expected.add(i.getId());
            });
            assertEquals(20, result.getValue().getSeed());
            assertEquals(expected, actual);
            return null;
        });
        pipeline.run();
    }
}