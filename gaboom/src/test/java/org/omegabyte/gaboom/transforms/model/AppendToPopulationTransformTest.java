package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class AppendToPopulationTransformTest {
    static final Random random = new Random();

    public static Individual<Integer> makeIndividual() {
        return new Individual<>(random, random.nextInt());
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testAppendToPopulationTransform() {
        Individuals<Integer> a = new Individuals<>(10);
        a.getIndividuals().add(makeIndividual());
        a.getIndividuals().add(makeIndividual());
        a.getIndividuals().add(makeIndividual());
        PCollection<KV<String, Individuals<Integer>>> aPCollection = pipeline.apply("Parents", Create.of(KV.of("test", a)));

        Individuals<Integer> b = new Individuals<>(20);
        b.getIndividuals().add(makeIndividual());
        b.getIndividuals().add(makeIndividual());
        b.getIndividuals().add(makeIndividual());
        PCollection<KV<String, Individuals<Integer>>> bPCollection = pipeline.apply(Create.of(KV.of("test", b)));

        PCollectionList<KV<String, Individuals<Integer>>> pCollectionList = PCollectionList.of(aPCollection).and(bPCollection);

        PCollection<KV<String, Individuals<Integer>>> output = pCollectionList.apply(new AppendToPopulationTransform<>());
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) kvs -> {
            KV<String, Individuals<Integer>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(20, result.getValue().getSeed());

            List<String> actual = new ArrayList<>();
            result.getValue().getIndividuals().forEach(i -> actual.add(i.getId()));

            List<String> expected = new ArrayList<>();
            a.getIndividuals().forEach(i -> expected.add(i.getId()));
            b.getIndividuals().forEach(i -> expected.add(i.getId()));
            assertEquals(expected, actual);
            return null;
        });
        pipeline.run();


    }

}