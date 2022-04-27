package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individual;

public class IndexIndividualFn<GenomeT> extends DoFn<Individual<GenomeT>, KV<String, Individual<GenomeT>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        Individual<GenomeT> individual = c.element();
        c.output(KV.of(individual.getId(), individual));
    }
}