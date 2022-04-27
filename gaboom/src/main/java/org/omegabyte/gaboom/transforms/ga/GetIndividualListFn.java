package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.List;

public class GetIndividualListFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, List<Individual<GenomeT>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output(c.element().getValue().getIndividuals());
    }
}

