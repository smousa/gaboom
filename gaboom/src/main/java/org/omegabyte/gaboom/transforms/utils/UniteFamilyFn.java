package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.ArrayList;
import java.util.List;

public class UniteFamilyFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
    private final TupleTag<Individuals<GenomeT>> firstGenTupleTag;
    private final TupleTag<Individuals<GenomeT>> nextGenTupleTag;

    public UniteFamilyFn(TupleTag<Individuals<GenomeT>> firstGenTupleTag, TupleTag<Individuals<GenomeT>> nextGenTupleTag) {
        this.firstGenTupleTag = firstGenTupleTag;
        this.nextGenTupleTag = nextGenTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        CoGbkResult result = c.element().getValue();

        Individuals<GenomeT> firstGen = result.getOnly(firstGenTupleTag);
        Individuals<GenomeT> nextGen = result.getOnly(nextGenTupleTag);

        List<Individual<GenomeT>> family = new ArrayList<>();
        family.addAll(firstGen.getIndividuals());
        family.addAll(nextGen.getIndividuals());
        c.output(KV.of(key, new Individuals<>(nextGen.getSeed(), family)));
    }
}