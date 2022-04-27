package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

public class CreateIndividualsFromIndividualFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
    private final TupleTag<Iterable<Individual<GenomeT>>> individualsTupleTag;
    private final TupleTag<BaseItem> baseItemTupleTag;

    public CreateIndividualsFromIndividualFn(TupleTag<Iterable<Individual<GenomeT>>> individualsTupleTag, TupleTag<BaseItem> baseItemTupleTag) {
        this.individualsTupleTag = individualsTupleTag;
        this.baseItemTupleTag = baseItemTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        CoGbkResult result = c.element().getValue();

        BaseItem baseItem = result.getOnly(baseItemTupleTag);
        Iterable<Individual<GenomeT>> individualIterator = result.getOnly(individualsTupleTag);

        Individuals<GenomeT> value = new Individuals<>(baseItem.getSeed());
        individualIterator.forEach(value.getIndividuals()::add);
        c.output(KV.of(key, value));
    }
}
