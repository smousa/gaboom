package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individuals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class ReplacePopulationAtIndexesFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
    private static final Logger logger = LoggerFactory.getLogger(ReplacePopulationAtIndexesFn.class);

    private final TupleTag<Individuals<GenomeT>> populationTT;
    private final TupleTag<Individuals<GenomeT>> selectedTT;
    private final TupleTag<List<Integer>> indexesTT;

    public ReplacePopulationAtIndexesFn(TupleTag<Individuals<GenomeT>> populationTT, TupleTag<Individuals<GenomeT>> selectedTT, TupleTag<List<Integer>> indexesTT) {
        this.populationTT = populationTT;
        this.selectedTT = selectedTT;
        this.indexesTT = indexesTT;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        CoGbkResult result = c.element().getValue();

        Individuals<GenomeT> population = result.getOnly(populationTT);
        Individuals<GenomeT> selected = result.getOnly(selectedTT);
        List<Integer> indexes = result.getOnly(indexesTT);

        if (selected.getIndividuals().size() != indexes.size()) {
            logger.error("Selection size does not equal size of indexes to replace: id={}", key);
            return;
        }

        for (int i = 0; i < indexes.size(); i++) {
            population.getIndividuals().remove(indexes.get(i));
            population.getIndividuals().add(indexes.get(i), selected.getIndividuals().get(i));
        }
        c.output(KV.of(key, new Individuals<>(selected.getSeed(), population.getIndividuals())));
    }
}