package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AppendToPopulationTransform<GenomeT extends Serializable> extends PTransform<PCollectionList<KV<String, Individuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
    static class AppendToPopulationFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
        private final TupleTag<Individuals<GenomeT>> originalTT;
        private final TupleTag<Individuals<GenomeT>> nextTT;

        public AppendToPopulationFn(TupleTag<Individuals<GenomeT>> originalTT, TupleTag<Individuals<GenomeT>> nextTT) {
            this.originalTT = originalTT;
            this.nextTT = nextTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            CoGbkResult result = c.element().getValue();

            Individuals<GenomeT> original = result.getOnly(originalTT);
            Individuals<GenomeT> next = result.getOnly(nextTT);

            List<Individual<GenomeT>> inds = new ArrayList<>();
            inds.addAll(original.getIndividuals());
            inds.addAll(next.getIndividuals());
            Individuals<GenomeT> value = new Individuals<>(next.getSeed(), inds);
            c.output(KV.of(key, value));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollectionList<KV<String, Individuals<GenomeT>>> input) {
        PCollection<KV<String, Individuals<GenomeT>>> result = input.get(0);
        for (int i = 1; i < input.size(); i++) {
            TupleTag<Individuals<GenomeT>> resultTT = new TupleTag<>();
            TupleTag<Individuals<GenomeT>> nextTT = new TupleTag<>();
            result = KeyedPCollectionTuple.of(resultTT, result)
                    .and(nextTT, input.get(i))
                    .apply(CoGroupByKey.create())
                    .apply(ParDo.of(new AppendToPopulationFn<>(resultTT, nextTT)));
        }
        return result;
    }
}
