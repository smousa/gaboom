package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.evaluate.*;

import java.io.Serializable;
import java.util.List;

public class Evaluate {

    public abstract class FitnessTransform<GenomeT extends Serializable> extends PTransform<PCollection<Individual<GenomeT>>, PCollection<Individual<GenomeT>>> {}

    public static class EvaluateTransform<GenomeT extends Serializable> extends PTransform<PCollection<KV<String, Individuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
        private final FitnessTransform<GenomeT> fitnessTransform;

        public EvaluateTransform(FitnessTransform<GenomeT> fitnessTransform) {
            this.fitnessTransform = fitnessTransform;
        }

        @Override
        public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
            // apply indexes based on the individual id and ignore evaluated individuals
            TupleTag<KV<String, String>> keyAtIdTT = new TupleTag<>();
            TupleTag<KV<String, BaseItem>> baseItemAtKeyTT = new TupleTag<>();
            TupleTag<KV<String, Individual<GenomeT>>> evaluatedAtKeyTT = new TupleTag<>();
            TupleTag<Individual<GenomeT>> notEvaluatedAtKeyTT = new TupleTag<>();
            PCollectionTuple result = input.apply(ParDo.of(new ExpandIndividualsFn<>(keyAtIdTT, baseItemAtKeyTT, evaluatedAtKeyTT))
                    .withOutputTags(notEvaluatedAtKeyTT, TupleTagList.of(keyAtIdTT).and(baseItemAtKeyTT).and(evaluatedAtKeyTT)));

            // perform the fitness transform
            PCollection<KV<String, Individual<GenomeT>>> indexedIndividualPCollection = result.get(notEvaluatedAtKeyTT)
                    .apply(fitnessTransform)
                    .apply(ParDo.of(new IndexIndividualFn<>()));

            // apply indexes based on input id
            PCollection<KV<String, Individual<GenomeT>>> evaluated = indexedIndividualPCollection.apply(AssignIndividualTransform.of(result.get(keyAtIdTT)));

            // combine all inputs into the sorted list
            PCollection<KV<String, List<Individual<GenomeT>>>> individualsPCollection = PCollectionList.of(evaluated).and(result.get(evaluatedAtKeyTT))
                    .apply(Flatten.pCollections())
                    .apply(Combine.perKey(new SortIndividualsFn<>()));

            // return the updated individuals
            TupleTag<BaseItem> baseItemTupleTag = new TupleTag<>();
            TupleTag<List<Individual<GenomeT>>> individualsTupleTag = new TupleTag<>();
            return KeyedPCollectionTuple.of(baseItemTupleTag, result.get(baseItemAtKeyTT))
                    .and(individualsTupleTag, individualsPCollection)
                    .apply(CoGroupByKey.create())
                    .apply(ParDo.of(new CreateIndividualsFn<>(baseItemTupleTag, individualsTupleTag)));
        }
    }

    public static <GenomeT extends Serializable> EvaluateTransform<GenomeT> as(FitnessTransform<GenomeT> fitnessTransform) {
        return new EvaluateTransform<>(fitnessTransform);
    }
}
