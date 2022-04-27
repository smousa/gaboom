package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

public class ExpandIndividualsFn<GenomeT> extends DoFn<KV<String, Individuals<GenomeT>>, Individual<GenomeT>> {
    private final TupleTag<KV<String, String>> indexTupleTag;
    private final TupleTag<KV<String, BaseItem>> baseItemTupleTag;

    public ExpandIndividualsFn(TupleTag<KV<String, String>> indexTupleTag, TupleTag<KV<String, BaseItem>> baseItemTupleTag) {
        this.indexTupleTag = indexTupleTag;
        this.baseItemTupleTag = baseItemTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Individuals<GenomeT> individuals = c.element().getValue();

        individuals.getIndividuals().forEach(ind -> {
            c.output(indexTupleTag, KV.of(ind.getId(), key));
            c.output(ind);
        });
        c.output(baseItemTupleTag, KV.of(key, individuals));
    }
}

