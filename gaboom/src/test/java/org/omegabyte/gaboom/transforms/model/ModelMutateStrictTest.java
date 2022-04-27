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
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Populate;
import org.omegabyte.gaboom.transforms.select.SelectTournamentFn;

import java.io.Serializable;

import static org.junit.Assert.*;

public class ModelMutateStrictTest {
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
        ModelTest.SECRET_WORD = "emancipated";
        input = pipeline.apply(Create.of(new BaseItem(seed)))
                .apply(Populate.as(new ModelTest.PopulateFn()).withPopSize(5))
                .apply(ParDo.of(new ToKVIndividuals()))
                .apply(Evaluate.as(new ModelTest.FitnessTransform()));
    }

    @Test
    public void testModelMutateStrict() {
        PCollection<KV<String, Individuals<String>>> output = input.apply(new ModelMutateStrict<>(
                new SelectTournamentFn<>(2),
                Mutate.as(new ModelTest.MutateFn(3)),
                new ModelTest.FitnessTransform(),
                2));
        PAssert.that(output).satisfies((SerializableFunction<Iterable<KV<String, Individuals<String>>>, Void>) kvs -> {
            KV<String, Individuals<String>> result = kvs.iterator().next();
            assertEquals("test", result.getKey());
            assertEquals(5, result.getValue().getIndividuals().size());

            Individual<String> ind;

            ind = result.getValue().getIndividuals().get(0);
            assertEquals("1r5rgf.yc4", ind.getId());
            assertEquals("farhfopmfoj", ind.getGenome());
            assertEquals(87.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(1);
            assertEquals("QsE9ng", ind.getId());
            assertEquals("xtspsjdgylu", ind.getGenome());
            assertEquals(110.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(2);
            assertEquals("5hpLmu", ind.getId());
            assertEquals("ybcwvmgnykr", ind.getGenome());
            assertEquals(112.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(3);
            assertEquals("O9PiSi", ind.getId());
            assertEquals("qlocfljbepo", ind.getGenome());
            assertEquals(88.0, ind.getFitness(), 0);

            ind = result.getValue().getIndividuals().get(4);
            assertEquals("8Ry8RZ", ind.getId());
            assertEquals("xvnjhpdqdxv", ind.getGenome());
            assertEquals(138.0, ind.getFitness(), 0);
            return null;
        });
        pipeline.run();

    }
}