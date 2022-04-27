package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.Random;

public class SplitIndividualsFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, Individuals<GenomeT>>> {
    private final TupleTag<KV<String, String>> idIndexTupleTag;
    private final TupleTag<KV<String, BaseItem>> baseItemIndexTupleTag;

    public SplitIndividualsFn(TupleTag<KV<String, String>> idIndexTupleTag, TupleTag<KV<String, BaseItem>> baseItemIndexTupleTag) {
        this.idIndexTupleTag = idIndexTupleTag;
        this.baseItemIndexTupleTag = baseItemIndexTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Individuals<GenomeT> individuals = c.element().getValue();
        Random rng = individuals.getRandomGenerator();

        c.output(baseItemIndexTupleTag, KV.of(key, new BaseItem(rng.nextLong())));
        individuals.getIndividuals().forEach(ind -> {
            c.output(idIndexTupleTag, KV.of(ind.getId(), key));

            Individuals<GenomeT> result = new Individuals<>(rng.nextLong());
            result.getIndividuals().add(ind);
            c.output(KV.of(ind.getId(), result));
        });

    }
}


