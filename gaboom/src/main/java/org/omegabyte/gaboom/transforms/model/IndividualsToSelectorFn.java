package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;

public class IndividualsToSelectorFn<GenomeT> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, SelectIndividuals<GenomeT>>> {
    private final int n;

    public IndividualsToSelectorFn(int n) {
        this.n = n;
    }

    public IndividualsToSelectorFn() {
        this.n = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Individuals<GenomeT> individuals = c.element().getValue();
        int n = this.n>0?this.n:individuals.getIndividuals().size();
        c.output(KV.of(key, new SelectIndividuals<>(individuals, n)));
    }
}