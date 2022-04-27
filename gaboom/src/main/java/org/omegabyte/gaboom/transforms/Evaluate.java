package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.utils.*;

import java.util.List;

public class Evaluate {

    public abstract class FitnessTransform<GenomeT> extends PTransform<PCollection<Individual<GenomeT>>, PCollection<Individual<GenomeT>>> {}

    public static class EvaluateTransform<GenomeT> extends PTransform<PCollection<KV<String, Individuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
        private final FitnessTransform<GenomeT> fitnessTransform;

        public EvaluateTransform(FitnessTransform<GenomeT> fitnessTransform) {
            this.fitnessTransform = fitnessTransform;
        }

        @Override
        public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {

            // apply indexes based on the individual id
            TupleTag<KV<String, String>> indexTupleTag = new TupleTag<>();
            TupleTag<KV<String, BaseItem>> baseItemIndexTupleTag = new TupleTag<>();
            TupleTag<Individual<GenomeT>> individualTupleTag = new TupleTag<>();

            PCollectionTuple result = input.apply(ParDo.of(new ExpandIndividualsFn<GenomeT>(indexTupleTag, baseItemIndexTupleTag))
                    .withOutputTags(individualTupleTag, TupleTagList.of(indexTupleTag).and(baseItemIndexTupleTag)));

            // perform the fitness transform
            PCollection<KV<String, Individual<GenomeT>>> indexedIndividualPCollection = result.get(individualTupleTag)
                    .apply(fitnessTransform)
                    .apply(ParDo.of(new IndexIndividualFn<>()));

            // apply indexes based on input id and combine as a sorted list
            TupleTag<String> idTupleTag = new TupleTag<>();
            TupleTag<Individual<GenomeT>> indexedIndividualTupleTag = new TupleTag<>();
            PCollection<KV<String, List<Individual<GenomeT>>>> individualsPCollection = KeyedPCollectionTuple.of(idTupleTag, result.get(indexTupleTag))
                    .and(indexedIndividualTupleTag, indexedIndividualPCollection)
                    .apply(CoGroupByKey.create())
                    .apply(ParDo.of(new AssignIndividualFn<>(idTupleTag, indexedIndividualTupleTag)))
                    .apply(Combine.perKey(new SortIndividualsFn<>()));

            // return the updated individuals
            TupleTag<BaseItem> baseItemTupleTag = new TupleTag<>();
            TupleTag<List<Individual<GenomeT>>> individualsTupleTag = new TupleTag<>();
            return KeyedPCollectionTuple.of(baseItemTupleTag, result.get(baseItemIndexTupleTag))
                    .and(individualsTupleTag, individualsPCollection)
                    .apply(CoGroupByKey.create())
                    .apply(ParDo.of(new CreateIndividualsFn<>(baseItemTupleTag, individualsTupleTag)));
        }
    }

    public static <GenomeT> EvaluateTransform<GenomeT> as(FitnessTransform<GenomeT> fitnessTransform) {
        return new EvaluateTransform<>(fitnessTransform);
    }
}
