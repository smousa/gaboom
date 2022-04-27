package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.ArrayList;
import java.util.List;

public class MergeIndividualsFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, List<Individual<GenomeT>>>> {
    private final TupleTag<String> idTupleTag;
    private final TupleTag<Individuals<GenomeT>> individualsTupleTag;

    public MergeIndividualsFn(TupleTag<String> idTupleTag, TupleTag<Individuals<GenomeT>> individualsTupleTag) {
        this.idTupleTag = idTupleTag;
        this.individualsTupleTag = individualsTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        CoGbkResult result = c.element().getValue();
        String key = result.getOnly(idTupleTag);
        List<Individual<GenomeT>> individuals = new ArrayList<>();
        result.getAll(individualsTupleTag).forEach(inds -> {
            individuals.addAll(inds.getIndividuals());
        });
        c.output(KV.of(key, individuals));
    }
}
