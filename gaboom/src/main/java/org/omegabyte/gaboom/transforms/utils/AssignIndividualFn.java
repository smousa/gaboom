package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;

public class AssignIndividualFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individual<GenomeT>>> {
    private final TupleTag<String> indexTupleTag;
    private final TupleTag<Individual<GenomeT>> individualTupleTag;

    public AssignIndividualFn(TupleTag<String> indexTupleTag, TupleTag<Individual<GenomeT>> individualTupleTag) {
        this.indexTupleTag = indexTupleTag;
        this.individualTupleTag = individualTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        CoGbkResult result = c.element().getValue();
        String key = result.getOnly(indexTupleTag);
        Individual<GenomeT> individual = result.getOnly(individualTupleTag);
        c.output(KV.of(key, individual));
    }
}


