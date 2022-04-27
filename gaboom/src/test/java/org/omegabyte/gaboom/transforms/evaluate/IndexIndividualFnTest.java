package org.omegabyte.gaboom.transforms.evaluate;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.omegabyte.gaboom.Individual;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class IndexIndividualFnTest {

    public static final Random random = new Random();

    public static Individual<Integer> makeIndividual() {
        return new Individual<>(random, random.nextInt());
    }

    @Test
    public void testIndexIndividualFn() throws Exception {
        DoFnTester<Individual<Integer>, KV<String, Individual<Integer>>> fnTester = DoFnTester.of(new IndexIndividualFn<>());
        List<KV<String, Individual<Integer>>> result = fnTester.processBundle(makeIndividual());
        result.forEach(r -> {
            assertEquals(r.getValue().getId(), r.getKey());
        });
    }
}