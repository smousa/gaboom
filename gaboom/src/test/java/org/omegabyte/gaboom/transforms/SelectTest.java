package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.select.SelectEliteFn;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SelectTest {
    private List<Individual<Integer>> individualsList;

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
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    // It should apply a selector on a set of individuals
    public void testSelectWithoutIndices() {
        Select.SelectFn<Integer> selectFn = new SelectEliteFn<>();
        SelectIndividuals<Integer> input = new SelectIndividuals<>(20, individualsList, 2);
        PCollection<KV<String, Individuals<Integer>>> output = pipeline.apply(Create.of(KV.of("test", input)))
                .apply(Select.as(selectFn));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) kvs -> {
            assertTrue(kvs.iterator().hasNext());
            KV<String, Individuals<Integer>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(input.getN(), result.getValue().getIndividuals().size());
            return null;
        });
        pipeline.run();
    }

    @Test
    // It should apply a selector on a set of individuals and return their indexes
    public void testSelectWithIndices() {
        TupleTag<KV<String, List<Integer>>> idx = new TupleTag<>();
        TupleTag<KV<String, Individuals<Integer>>> ind = new TupleTag<>();

        Select.SelectFn<Integer> selectFn = new SelectEliteFn<>();
        SelectIndividuals<Integer> input = new SelectIndividuals<>(20, individualsList, 2);
        PCollectionTuple output = pipeline.apply(Create.of(KV.of("test", input)))
                .apply(Select.as(selectFn, idx, ind));
        PAssert.that(output.get(idx)).satisfies((SerializableFunction<Iterable<KV<String, List<Integer>>>, Void>) kvs -> {
            KV<String, List<Integer>> result = kvs.iterator().next();
            assertEquals(input.getN(), result.getValue().size());
            return null;
        });
        PAssert.that(output.get(ind)).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) kvs -> {
            KV<String, Individuals<Integer>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(input.getN(), result.getValue().getIndividuals().size());
            return null;
        });
        pipeline.run();
    }
}