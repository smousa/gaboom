package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.evaluate.AssignIndividualTransform;
import org.omegabyte.gaboom.transforms.evaluate.CreateIndividualsTransform;
import org.omegabyte.gaboom.transforms.evaluate.IndexIndividualFn;
import org.omegabyte.gaboom.transforms.evaluate.SortIndividualsFn;

import java.io.Serializable;
import java.util.Random;

public class Evaluate {

    public abstract class FitnessTransform<GenomeT extends Serializable> extends PTransform<PCollection<Individual<GenomeT>>, PCollection<Individual<GenomeT>>> {}

    public static class EvaluateTransform<GenomeT extends Serializable> extends PTransform<PCollection<KV<String, Individuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
        private final FitnessTransform<GenomeT> fitnessTransform;

        public EvaluateTransform(FitnessTransform<GenomeT> fitnessTransform) {
            this.fitnessTransform = fitnessTransform;
        }

        static class ExpandIndividualsFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, Individual<GenomeT>> {
            private final TupleTag<KV<String, String>> keyAtIdTT;
            private final TupleTag<KV<String, BaseItem>> baseItemAtKeyTT;
            private final TupleTag<KV<String, Individual<GenomeT>>> evaluatedAtKeyTT;

            public ExpandIndividualsFn(TupleTag<KV<String, String>> keyAtIdTT, TupleTag<KV<String, BaseItem>> baseItemAtKeyTT, TupleTag<KV<String, Individual<GenomeT>>> evaluatedAtKeyTT) {
                this.keyAtIdTT = keyAtIdTT;
                this.baseItemAtKeyTT = baseItemAtKeyTT;
                this.evaluatedAtKeyTT = evaluatedAtKeyTT;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                String key = c.element().getKey();
                Individuals<GenomeT> individuals = c.element().getValue();

                Random r = new Random();
                r.setSeed(individuals.getSeed());

                c.output(baseItemAtKeyTT, KV.of(key, new BaseItem(r.nextLong())));
                individuals.getIndividuals().forEach(i -> {
                    if (i.getFitness() != null) {
                        c.output(evaluatedAtKeyTT, KV.of(key, i));
                        return;
                    }

                    c.output(keyAtIdTT, KV.of(i.getId(), key));
                    c.output(i);
                });
            }
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

            // combine all inputs into the sorted list and return the updated individuals
            return PCollectionList.of(evaluated).and(result.get(evaluatedAtKeyTT))
                    .apply(Flatten.pCollections())
                    .apply(Combine.perKey(new SortIndividualsFn<>()))
                    .apply(CreateIndividualsTransform.of(result.get(baseItemAtKeyTT)));
        }
    }

    public static <GenomeT extends Serializable> EvaluateTransform<GenomeT> as(FitnessTransform<GenomeT> fitnessTransform) {
        return new EvaluateTransform<>(fitnessTransform);
    }
}
