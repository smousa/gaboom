package org.omegabyte.gaboom.transforms.model;

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

import java.util.*;

import static org.junit.Assert.assertEquals;

public class IndividualsFromIndividualTransformTest {

    final static Random random = new Random();
    public static KV<String, Individual<Integer>> makeIndividual(String key) {
        return KV.of(key, new Individual<Integer>(random, random.nextInt()));
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testIndividualsFromIndividualTransform() {

        List<KV<String, Individual<Integer>>> individualList = new ArrayList<>();
        individualList.add(makeIndividual("test"));
        individualList.add(makeIndividual("test"));
        individualList.add(makeIndividual("test"));

        PCollection<KV<String, BaseItem>> baseItemPCollection = pipeline.apply("BaseItem", Create.of(KV.of("test", new BaseItem(100))));
        PCollection<KV<String, Individuals<Integer>>> output = pipeline
                .apply(Create.of(individualList))
                .apply(new IndividualsFromIndividualTransform<>(baseItemPCollection));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) kvs -> {
            KV<String, Individuals<Integer>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(100, result.getValue().getSeed());

            Set<String> actual = new HashSet<>();
            result.getValue().getIndividuals().forEach(i->actual.add(i.getId()));

            Set<String> expected = new HashSet<>();
            individualList.forEach(i-> expected.add(i.getValue().getId()));

            assertEquals(expected, actual);
            return null;
        });
        pipeline.run();
    }

}