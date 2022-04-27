package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class ReplacePopulationTransformTest {

    final static Random random = new Random();
    public static Individual<Integer> makeIndividual() {
        return new Individual<>(random, random.nextInt());
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testReplacePopulationTransform() {
        Individuals<Integer> originalPopulation = new Individuals<>(0);
        for (int i = 0; i < 10; i++) {
            originalPopulation.getIndividuals().add(makeIndividual());
        }
        PCollection<KV<String, Individuals<Integer>>> originalPopulationPCollection = pipeline.apply("OriginalPopulation", Create.of(KV.of("test", originalPopulation)));

        Individuals<Integer> selectedPopulation = new Individuals<>(20);
        for (int i = 0; i < 2; i++) {
            selectedPopulation.getIndividuals().add(makeIndividual());
        }
        PCollection<KV<String, Individuals<Integer>>> selectedPopulationPCollection = pipeline.apply("SelectedPopulation", Create.of(KV.of("test", selectedPopulation)));

        List<Integer> indices = new ArrayList<>();
        indices.add(2);
        indices.add(9);
        PCollection<KV<String, List<Integer>>> selectedIndicesPCollection = pipeline.apply("SelectedIndices", Create.of(KV.of("test", indices)));

        PCollection<KV<String, Individuals<Integer>>> output = originalPopulationPCollection.apply(ReplacePopulationTransform.of(selectedPopulationPCollection, selectedIndicesPCollection));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) kvs -> {
            KV<String, Individuals<Integer>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());

            List<String> actual = new ArrayList<>();
            result.getValue().getIndividuals().forEach(i -> actual.add(i.getId()));

            List<String> expected = new ArrayList<>();
            expected.add(originalPopulation.getIndividuals().get(0).getId());
            expected.add(originalPopulation.getIndividuals().get(1).getId());
            expected.add(selectedPopulation.getIndividuals().get(0).getId());
            expected.add(originalPopulation.getIndividuals().get(3).getId());
            expected.add(originalPopulation.getIndividuals().get(4).getId());
            expected.add(originalPopulation.getIndividuals().get(5).getId());
            expected.add(originalPopulation.getIndividuals().get(6).getId());
            expected.add(originalPopulation.getIndividuals().get(7).getId());
            expected.add(originalPopulation.getIndividuals().get(8).getId());
            expected.add(selectedPopulation.getIndividuals().get(1).getId());
            assertEquals(expected, actual);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testReplacePopulationTransformInvalidSize() {
        Individuals<Integer> originalPopulation = new Individuals<>(0);
        for (int i = 0; i < 10; i++) {
            originalPopulation.getIndividuals().add(makeIndividual());
        }
        PCollection<KV<String, Individuals<Integer>>> originalPopulationPCollection = pipeline.apply("OriginalPopulation", Create.of(KV.of("test", originalPopulation)));

        Individuals<Integer> selectedPopulation = new Individuals<>(20);
        for (int i = 0; i < 2; i++) {
            selectedPopulation.getIndividuals().add(makeIndividual());
        }
        PCollection<KV<String, Individuals<Integer>>> selectedPopulationPCollection = pipeline.apply("SelectedPopulation", Create.of(KV.of("test", selectedPopulation)));

        List<Integer> indices = new ArrayList<>();
        indices.add(2);
        PCollection<KV<String, List<Integer>>> selectedIndicesPCollection = pipeline.apply("SelectedIndices", Create.of(KV.of("test", indices)));

        PCollection<KV<String, Individuals<Integer>>> output = originalPopulationPCollection.apply(ReplacePopulationTransform.of(selectedPopulationPCollection, selectedIndicesPCollection));
        PAssert.that(output).empty();
        pipeline.run();
    }
}