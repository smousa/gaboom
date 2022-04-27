package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReplacePopulationTransform<GenomeT extends Serializable> extends PTransform<PCollection<KV<String,Individuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
    private final PCollection<KV<String, Individuals<GenomeT>>> selected;
    private final PCollection<KV<String, List<Integer>>> indices;

    public ReplacePopulationTransform(PCollection<KV<String, Individuals<GenomeT>>> selected, PCollection<KV<String, List<Integer>>> indices) {
        this.selected = selected;
        this.indices = indices;
    }

    public static <GenomeT extends Serializable> ReplacePopulationTransform<GenomeT> of(PCollection<KV<String, Individuals<GenomeT>>> selected, PCollection<KV<String, List<Integer>>> indices) {
        return new ReplacePopulationTransform<>(selected, indices);
    }

    static class ReplacePopulationFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
        private static Logger logger = LoggerFactory.getLogger(ReplacePopulationFn.class);

        private final TupleTag<Individuals<GenomeT>> populationTT;
        private final TupleTag<Individuals<GenomeT>> selectedTT;
        private final TupleTag<List<Integer>> indicesTT;

        public ReplacePopulationFn(TupleTag<Individuals<GenomeT>> populationTT, TupleTag<Individuals<GenomeT>> selectedTT, TupleTag<List<Integer>> indicesTT) {
            this.populationTT = populationTT;
            this.selectedTT = selectedTT;
            this.indicesTT = indicesTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            CoGbkResult result = c.element().getValue();

            List<Individual<GenomeT>> population = new ArrayList<>(result.getOnly(populationTT).getIndividuals());
            Individuals<GenomeT> selected = result.getOnly(selectedTT);
            List<Integer> indices = result.getOnly(indicesTT);

            if (selected.getIndividuals().size() != indices.size()) {
                logger.error("Selection size does not equal size of indices to replace: id={}", key);
                return;
            }

            for (int i = 0; i < indices.size(); i++) {
                population.set(indices.get(i), selected.getIndividuals().get(i));
            }
            c.output(KV.of(key, new Individuals<>(selected.getSeed(), population)));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        TupleTag<Individuals<GenomeT>> populationTT = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> selectedTT = new TupleTag<>();
        TupleTag<List<Integer>> indicesTT = new TupleTag<>();
        return KeyedPCollectionTuple.of(populationTT, input).and(selectedTT, selected).and(indicesTT, indices)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReplacePopulationFn<>(populationTT, selectedTT, indicesTT)));
    }
}