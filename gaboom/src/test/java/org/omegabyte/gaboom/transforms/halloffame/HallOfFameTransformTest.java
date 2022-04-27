package org.omegabyte.gaboom.transforms.halloffame;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.omegabyte.gaboom.transforms.halloffame.HallOfFameFnTest.makeIndividual;

public class HallOfFameTransformTest {

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testHallOfFameTransform() {
        List<Individual<Integer>> a = new ArrayList<>();
        a.add(makeIndividual(0.2));
        a.add(makeIndividual(0.3));
        a.add(makeIndividual(0.4));
        PCollection<List<Individual<Integer>>> aPCollection = pipeline.apply("HallOfFame", Create.of(Arrays.asList(a)));

        List<Individual<Integer>> b = new ArrayList<>();
        b.add(makeIndividual(0.25));
        b.add(makeIndividual(0.26));
        b.add(makeIndividual(0.45));
        b.add(makeIndividual(0.52));
        PCollection<List<Individual<Integer>>> bPCollection = pipeline.apply(Create.of(Arrays.asList(b)));

        PCollection<List<Individual<Integer>>> output = bPCollection.apply(new HallOfFameTransform<>(aPCollection, 5));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<List<Individual<Integer>>>, Void>) lists -> {
            List<Individual<Integer>> result = lists.iterator().next();
            List<Double> expected = Arrays.asList(.2, .25, .26, .3, .4);
            List<Double> actual = new ArrayList<>();
            result.forEach(i -> actual.add(i.getFitness()));
            assertEquals(expected, actual);
            return null;
        });
        pipeline.run();
    }

}