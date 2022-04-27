package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Population;

import java.io.Serializable;

public class MapPopulationFn<GenomeT extends Serializable> extends DoFn<Population<GenomeT>, KV<String, Population<GenomeT>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output(KV.of(c.element().getId(), c.element()));
    }
}