package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import sun.security.krb5.internal.PAData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class MutateTest {

    static class SimpleMutateFn extends Mutate.MutateFn<Integer> implements Serializable {
        @Override
        public Integer mutate(ProcessContext context, Random random, Integer genome) {
            return genome+1;
        }
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    // It should mutate a population based on its mutation rate
    public void testMutateTransform() {
        List<Individual<Integer>> individualsList = new ArrayList<>();
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

        Mutate.MutateFn<Integer> mutateFn = new SimpleMutateFn();
        PCollectionView<Integer> sideInput = pipeline.apply("SideInput", Create.of(4))
                .apply(View.asSingleton());

        PCollection<KV<String, Individuals<Integer>>> population = pipeline
                .apply(Create.of(KV.of("test", new Individuals<>(0, individualsList))))
                .apply(Mutate.as(mutateFn).withMutRate(.5).withSideInputs(sideInput));
        PAssert.that(population).satisfies((SerializableFunction<Iterable<KV<String, Individuals<Integer>>>, Void>) output -> {
            KV<String, Individuals<Integer>> result = output.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(-4962768465676381896L, result.getValue().getSeed());

            assertEquals(individualsList.get(0).getId(), result.getValue().getIndividuals().get(0).getId());
            assertEquals(individualsList.get(0).getGenome() + 1, (int) result.getValue().getIndividuals().get(0).getGenome());

            assertEquals(individualsList.get(1).getId(), result.getValue().getIndividuals().get(1).getId());
            assertEquals(individualsList.get(1).getGenome(), result.getValue().getIndividuals().get(1).getGenome());

            assertEquals(individualsList.get(2).getId(), result.getValue().getIndividuals().get(2).getId());
            assertEquals(individualsList.get(2).getGenome(), result.getValue().getIndividuals().get(2).getGenome());

            assertEquals(individualsList.get(3).getId(), result.getValue().getIndividuals().get(3).getId());
            assertEquals(individualsList.get(3).getGenome(), result.getValue().getIndividuals().get(3).getGenome());
            return null;
        });
        pipeline.run();
    }
}