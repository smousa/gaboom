package org.omegabyte.gaboom.transforms.model;

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
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Populate;
import org.omegabyte.gaboom.transforms.select.SelectTournamentFn;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class ModelSteadyStateStrictTest {
    private static final int seed = 0;
    private PCollection<KV<String, Individuals<String>>> input;

    static class ToKVIndividuals extends DoFn<Individuals<String>, KV<String, Individuals<String>>> implements Serializable {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(KV.of("test", c.element()));
        }
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Before
    public void setUpTest() {
        ModelTest.SECRET_WORD = "hogwash";
        input = pipeline.apply(Create.of(new BaseItem(seed)))
                .apply(Populate.as(new ModelTest.PopulateFn()).withPopSize(5))
                .apply(ParDo.of(new ToKVIndividuals()))
                .apply(Evaluate.as(new ModelTest.FitnessTransform()));
    }

    @Test
    public void testModelSteadyStateStrict() {
        PCollection<KV<String, Individuals<String>>> output = input.apply(new ModelSteadyStateStrict<>(
                new SelectTournamentFn<>(2),
                Crossover.as(new ModelTest.CrossoverFn()).withCrossRate(0.8),
                Mutate.as(new ModelTest.MutateFn(3)).withMutRate(0.8),
                new ModelTest.FitnessTransform()));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<String>>>, Void>) kvs -> {
            KV<String, Individuals<String>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(5, result.getValue().getIndividuals().size());

            Individual<String> ind;

            ind = result.getValue().getIndividuals().get(0);
            assertEquals("CCKEN2.Pux", ind.getId());
            assertEquals("jhgveme", ind.getGenome());
            assertEquals(23.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(1);
            assertEquals("YZMO9P", ind.getId());
            assertEquals("locfljb", ind.getGenome());
            assertEquals(51.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(2);
            assertEquals("KhLr8R", ind.getId());
            assertEquals("xvnjhpd", ind.getGenome());
            assertEquals(57.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(3);
            assertEquals("sE9ngG", ind.getId());
            assertEquals("jdgylue", ind.getGenome());
            assertEquals(31.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(4);
            assertEquals("6hYkdm", ind.getId());
            assertEquals("astvybc", ind.getGenome());
            assertEquals(71.0, ind.getFitness(), 0);
            return null;
        });
        pipeline.run();
    }
}