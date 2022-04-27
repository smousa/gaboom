package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.List;

public class CreateIndividualsFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
    private final TupleTag<BaseItem> baseItemTupleTag;
    private final TupleTag<List<Individual<GenomeT>>> individualsTupleTag;

    public CreateIndividualsFn(TupleTag<BaseItem> baseItemTupleTag, TupleTag<List<Individual<GenomeT>>> individualsTupleTag) {
        this.baseItemTupleTag = baseItemTupleTag;
        this.individualsTupleTag = individualsTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        CoGbkResult result = c.element().getValue();
        BaseItem baseItem = result.getOnly(baseItemTupleTag);
        List<Individual<GenomeT>> individuals = result.getOnly(individualsTupleTag);
        c.output(KV.of(key, new Individuals<>(baseItem.getSeed(), individuals)));
    }
}