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

import java.io.Serializable;

import static org.junit.Assert.*;

public class ModelRingTest {
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
        ModelTest.SECRET_WORD = "keystone";
        input = pipeline.apply(Create.of(new BaseItem(seed)))
                .apply(Populate.as(new ModelTest.PopulateFn()).withPopSize(5))
                .apply(ParDo.of(new ToKVIndividuals()))
                .apply(Evaluate.as(new ModelTest.FitnessTransform()));
    }

    @Test
    public void testModelRing() {
        PCollection<KV<String, Individuals<String>>> output = input.apply(new ModelRing<>(
                Crossover.as(new ModelTest.CrossoverFn()),
                Mutate.as(new ModelTest.MutateFn(3)).withMutRate(0.5),
                new ModelTest.FitnessTransform()
        )).apply("Evaluate", Evaluate.as(new ModelTest.FitnessTransform()));

        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<String>>>, Void>) kvs -> {
            KV<String, Individuals<String>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(5, result.getValue().getIndividuals().size());

            Individual<String> ind;

            ind = result.getValue().getIndividuals().get(0);
            assertEquals("PiSiKy", ind.getId());
            assertEquals("ljbepowf", ind.getGenome());
            assertEquals(57.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(1);
            assertEquals("aohYfB", ind.getId());
            assertEquals("ltvypcwv", ind.getGenome());
            assertEquals(67.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(2);
            assertEquals("3Ozvcy.D8E", ind.getId());
            assertEquals("vgnyhpdu", ind.getGenome());
            assertEquals(69.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(3);
            assertEquals("gGfOKN", ind.getId());
            assertEquals("yluegqfl", ind.getGenome());
            assertEquals(69.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(4);
            assertEquals("GSnGWs", ind.getId());
            assertEquals("xvzjkpdt", ind.getGenome());
            assertEquals(75.0, ind.getFitness(), 0);
            return null;
        });
        pipeline.run();
    }

}