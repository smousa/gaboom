package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.omegabyte.gaboom.Population;

import static org.junit.Assert.assertEquals;

public class MapPopulationFnTest {

    @Test
    // It should map populations by their id
    public void testMapPopulationFn() throws Exception {
        Population<Integer> population = new Population<>("testpop");

        DoFnTester<Population<Integer>, KV<String, Population<Integer>>> fnTester = DoFnTester.of(new MapPopulationFn<>());
        assertEquals(KV.of(population.getId(), population), fnTester.processBundle(population).get(0));
    }

}