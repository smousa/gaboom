package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IndividualsFromIndividualFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
    private final TupleTag<Individual<GenomeT>> individualTT;
    private final TupleTag<BaseItem> baseItemTT;

    public IndividualsFromIndividualFn(TupleTag<Individual<GenomeT>> individualTT, TupleTag<BaseItem> baseItemTT) {
        this.individualTT = individualTT;
        this.baseItemTT = baseItemTT;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        CoGbkResult result = c.element().getValue();

        List<Individual<GenomeT>> individualList = new ArrayList<>();
        result.getAll(individualTT).forEach(individualList::add);
        c.output(KV.of(key, new Individuals<>(result.getOnly(baseItemTT).getSeed(), individualList)));
    }
}