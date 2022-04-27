package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;

public class Individuals2SelectIndividualsFn<GenomeT> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, SelectIndividuals<GenomeT>>> {
    private TupleTag<KV<String, Integer>> sizeTupleTag = new TupleTag<>();
    private int size = 0;

    public Individuals2SelectIndividualsFn() {}

    public Individuals2SelectIndividualsFn(int size) {
        this.size = size;
    }

    public Individuals2SelectIndividualsFn(TupleTag<KV<String, Integer>> sizeTupleTag, int size) {
        this.sizeTupleTag = sizeTupleTag;
        this.size = size;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Individuals<GenomeT> individuals = c.element().getValue();

        int size = this.size>0?this.size:individuals.getIndividuals().size();
        c.output(sizeTupleTag, KV.of(key, individuals.getIndividuals().size()));
        c.output(KV.of(key, new SelectIndividuals<>(individuals, size)));
    }
}