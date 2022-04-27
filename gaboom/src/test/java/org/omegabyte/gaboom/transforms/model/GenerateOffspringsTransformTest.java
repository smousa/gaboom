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
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Populate;
import org.omegabyte.gaboom.transforms.select.SelectTournamentFn;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GenerateOffspringsTransformTest {
    private static final int seed = 0;
    private PCollection<KV<String, SelectIndividuals<String>>> input;

    static class ToKVSelectIndividuals extends DoFn<KV<String, Individuals<String>>, KV<String, SelectIndividuals<String>>> implements Serializable {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(KV.of(c.element().getKey(), new SelectIndividuals<>(c.element().getValue(), 3)));
        }
    }

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
        ModelTest.SECRET_WORD = "sobriety";
        input = pipeline.apply(Create.of(new BaseItem(seed)))
                .apply(Populate.as(new ModelTest.PopulateFn()).withPopSize(10))
                .apply(ParDo.of(new ToKVIndividuals()))
                .apply(Evaluate.as(new ModelTest.FitnessTransform()))
                .apply(ParDo.of(new ToKVSelectIndividuals()));
    }

    @Test
    public void testGenerateOffspringsTransformBadSelection() {
        PCollection<KV<String, Individuals<String>>> output = input.apply(GenerateOffspringsTransform.of(
                new SelectTournamentFn<>(10),
                Crossover.as(new ModelTest.CrossoverFn()).withCrossRate(0.5),
                Mutate.as(new ModelTest.MutateFn(2)).withMutRate(0.5)));
        PAssert.that(output).empty();
        pipeline.run();
    }

    @Test
    public void testGenerateOffspringsTransform() {
        PCollection<KV<String, Individuals<String>>> output = input.apply(GenerateOffspringsTransform.of(
                new SelectTournamentFn<>(3),
                Crossover.as(new ModelTest.CrossoverFn()).withCrossRate(0.5),
                Mutate.as(new ModelTest.MutateFn(2)).withMutRate(0.5)));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<String>>>, Void>) kvs -> {
            KV<String, Individuals<String>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(3, result.getValue().getIndividuals().size());

            Individual<String> ind;

            ind = result.getValue().getIndividuals().get(0);
            assertEquals("mjMdXk", ind.getId());
            assertEquals("stvyhcwv", ind.getGenome());
            assertNull(ind.getFitness());

            ind = result.getValue().getIndividuals().get(1);
            assertEquals("0ml7V3", ind.getId());
            assertEquals("xvvjhpwv", ind.getGenome());
            assertNull(ind.getFitness());

            ind = result.getValue().getIndividuals().get(2);
            assertEquals("IzyfWT", ind.getId());
            assertEquals("onjvxzxz", ind.getGenome());
            assertEquals((Double) 58.0, ind.getFitness());
            return null;
        });
        pipeline.run();
    }

}