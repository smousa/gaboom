package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ReplaceIndividualsFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
    private static final Logger logger = LoggerFactory.getLogger(ReplaceIndividualsFn.class);

    private final TupleTag<Individuals<GenomeT>> firstGenTupleTag;
    private final TupleTag<Individuals<GenomeT>> nextGenTupleTag;
    private final TupleTag<List<Integer>> indicesTupleTag;

    public ReplaceIndividualsFn(TupleTag<Individuals<GenomeT>> firstGenTupleTag, TupleTag<Individuals<GenomeT>> nextGenTupleTag, TupleTag<List<Integer>> indicesTupleTag) {
        this.firstGenTupleTag = firstGenTupleTag;
        this.nextGenTupleTag = nextGenTupleTag;
        this.indicesTupleTag = indicesTupleTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        CoGbkResult result = c.element().getValue();

        Individuals<GenomeT> firstGen = result.getOnly(firstGenTupleTag);
        Individuals<GenomeT> nextGen = result.getOnly(nextGenTupleTag);
        List<Integer> indices = result.getOnly(indicesTupleTag);

        if (indices.size() != nextGen.getIndividuals().size()) {
            logger.error("indices do not match selected group {}", key);
            return;
        }

        List<Individual<GenomeT>> output = firstGen.getIndividuals();
        for (int i = 0; i < indices.size(); i++) {
            output.remove((int) indices.get(i));
            output.add(indices.get(i), nextGen.getIndividuals().get(i));
        }
        c.output(KV.of(key, new Individuals<>(nextGen.getSeed(), output)));
    }
}