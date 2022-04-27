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
import org.omegabyte.gaboom.transforms.select.SelectEliteFn;
import org.omegabyte.gaboom.transforms.select.SelectTournamentFn;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class ModelDownToSizeTest {
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
        ModelTest.SECRET_WORD = "animosity";
        input = pipeline.apply(Create.of(new BaseItem(seed)))
                .apply(Populate.as(new ModelTest.PopulateFn()).withPopSize(5))
                .apply(ParDo.of(new ToKVIndividuals()))
                .apply(Evaluate.as(new ModelTest.FitnessTransform()));
    }

    @Test
    public void testModelDownToSize() {
        PCollection<KV<String, Individuals<String>>> output = input.apply(new ModelDownToSize<>(
                new SelectTournamentFn<>(3),
                new SelectEliteFn<>(),
                Crossover.as(new ModelTest.CrossoverFn()).withCrossRate(0.5),
                Mutate.as(new ModelTest.MutateFn(3)).withMutRate(0.8),
                new ModelTest.FitnessTransform(),
                4
        ));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<String>>>, Void>) kvs -> {
            KV<String, Individuals<String>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(5, result.getValue().getIndividuals().size());

            Individual<String> ind;

            ind = result.getValue().getIndividuals().get(0);
            assertEquals("y7H0e1", ind.getId());
            assertEquals("powfnsomy", ind.getGenome());
            assertEquals((Double) 51.0, ind.getFitness());

            ind = result.getValue().getIndividuals().get(1);
            assertEquals("MdXk0m", ind.getId());
            assertEquals("powfxsoms", ind.getGenome());
            assertEquals((Double) 65.0, ind.getFitness());

            ind = result.getValue().getIndividuals().get(2);
            assertEquals("l7V350.ePe", ind.getId());
            assertEquals("pokjxsomc", ind.getGenome());
            assertEquals((Double) 65.0, ind.getFitness());

            ind = result.getValue().getIndividuals().get(3);
            assertEquals("7vOGJI", ind.getId());
            assertEquals("zxkgxtsps", ind.getGenome());
            assertEquals((Double) 73.0, ind.getFitness());

            ind = result.getValue().getIndividuals().get(4);
            assertEquals("KNrHVY", ind.getId());
            assertEquals("egqflaqlo", ind.getGenome());
            assertEquals((Double) 73.0, ind.getFitness());
            return null;
        });
        pipeline.run();
    }
}