package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ModelRing<GenomeT extends Serializable> extends ModelTransform<GenomeT> {
    private final Select.SelectNoIndexTransform<GenomeT> selectTransform;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;

    public ModelRing(Select.SelectFn<GenomeT> selectFn, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform) {
        this.selectTransform = Select.as(selectFn);
        this.crossoverTransform = crossoverTransform.withCrossRate(1);
        this.mutateTransform = mutateTransform;
        this.evaluateTransform = Evaluate.as(fitnessTransform);
    }

    static class ModelRingFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, Individuals<GenomeT>>> {
        private final TupleTag<KV<String, String>> indexTupleTag;
        private final TupleTag<KV<String, BaseItem>> baseItemTupleTag;
        private final TupleTag<KV<String, Individuals<GenomeT>>> parentTupleTag;

        public ModelRingFn(TupleTag<KV<String, String>> indexTupleTag, TupleTag<KV<String, BaseItem>> baseItemTupleTag, TupleTag<KV<String, Individuals<GenomeT>>> parentTupleTag) {
            this.indexTupleTag = indexTupleTag;
            this.baseItemTupleTag = baseItemTupleTag;
            this.parentTupleTag = parentTupleTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            Individuals<GenomeT> parents = c.element().getValue();
            Random rng = parents.getRandomGenerator();
            int parentSize = parents.getIndividuals().size();

            c.output(baseItemTupleTag, KV.of(key, new BaseItem(rng.nextLong())));
            for (int i = 0; i < parentSize; i++) {
                Individual<GenomeT> parent = parents.getIndividuals().get(i);
                c.output(indexTupleTag, KV.of(parent.getId(), key));

                List<Individual<GenomeT>> plist = new ArrayList<>();
                plist.add(parent);
                c.output(parentTupleTag, KV.of(parent.getId(), new Individuals<>(parents.getSeed(), plist)));

                Individuals<GenomeT> crossoverIndividuals = new Individuals<>(rng.nextLong());
                crossoverIndividuals.getIndividuals().add(parent);
                crossoverIndividuals.getIndividuals().add(parents.getIndividuals().get((i+1)%parentSize));
                c.output(KV.of(parent.getId(), crossoverIndividuals));
            }
        }
    }

    static class GetBestIndividualFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individual<GenomeT>>> {
        private final TupleTag<Individuals<GenomeT>> rankedTT;
        private final TupleTag<String> keyTT;

        public GetBestIndividualFn(TupleTag<Individuals<GenomeT>> rankedTT, TupleTag<String> keyTT) {
            this.rankedTT = rankedTT;
            this.keyTT = keyTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            CoGbkResult result = c.element().getValue();
            String key = result.getOnly(keyTT);
            c.output(KV.of(key, result.getOnly(rankedTT).getIndividuals().get(0)));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        // Set up indexes for each selection
        TupleTag<KV<String, String>> keyAtIdTT = new TupleTag<>();
        TupleTag<KV<String, BaseItem>> baseItemAtKeyTT = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> originalIndividualAtIdTT = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> crossoverIndividualsAtIdTT = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new ModelRingFn<>(keyAtIdTT, baseItemAtKeyTT, originalIndividualAtIdTT))
                .withOutputTags(crossoverIndividualsAtIdTT,
                        TupleTagList.of(keyAtIdTT)
                                .and(baseItemAtKeyTT)
                                .and(originalIndividualAtIdTT)));

        // Make offsprings
        PCollection<KV<String, Individuals<GenomeT>>> offsprings = result.get(crossoverIndividualsAtIdTT)
                .apply(crossoverTransform)
                .apply(mutateTransform);

        // Append the offsprings back into the original parent and evaluate
        PCollection<KV<String, Individuals<GenomeT>>> ranked = PCollectionList.of(result.get(originalIndividualAtIdTT)).and(offsprings)
                .apply(new AppendToPopulationTransform<>())
                .apply(evaluateTransform);

        // Get the best individual and restore its index
        TupleTag<Individuals<GenomeT>> rankedTT = new TupleTag<>();
        TupleTag<String> keyTT = new TupleTag<>();
        return KeyedPCollectionTuple
                .of(rankedTT, ranked)
                .and(keyTT, result.get(keyAtIdTT))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new GetBestIndividualFn<>(rankedTT, keyTT)))
                .apply(new IndividualsFromIndividualTransform<>(result.get(baseItemAtKeyTT)));
    }
}