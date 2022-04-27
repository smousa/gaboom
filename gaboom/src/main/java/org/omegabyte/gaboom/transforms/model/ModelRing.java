package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.CrossoverIndividuals;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;
import org.omegabyte.gaboom.transforms.utils.CreateIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.Individuals2SelectIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.MergeIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.UniteFamilyFn;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ModelRing<GenomeT> extends ModelTransform<GenomeT> {
    private final Select.SelectNoIndexTransform<GenomeT> selectTransform;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.FitnessTransform<GenomeT> fitnessTransform;

    public ModelRing(Select.SelectFn<GenomeT> selectFn, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform) {
        this.selectTransform = Select.as(selectFn);
        this.crossoverTransform = crossoverTransform.withCrossRate(1);
        this.mutateTransform = mutateTransform;
        this.fitnessTransform = fitnessTransform;
    }

    static class ModelRingFn<GenomeT> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, CrossoverIndividuals<GenomeT>>> {
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

            c.output(baseItemTupleTag, KV.of(key, new BaseItem(rng.nextLong())));
            for (int i = 0; i < parents.getIndividuals().size(); i++) {
                Individual<GenomeT> parent = parents.getIndividuals().get(i);
                c.output(indexTupleTag, KV.of(parent.getId(), key));

                List<Individual<GenomeT>> plist = new ArrayList<>();
                plist.add(parent);
                c.output(parentTupleTag, KV.of(parent.getId(), new Individuals<>(parents.getSeed(), plist)));

                CrossoverIndividuals<GenomeT> crossoverIndividuals = new CrossoverIndividuals<>(rng.nextLong(), parent,
                        parents.getIndividuals().get((i+1)%parents.getIndividuals().size()));
                c.output(KV.of(parent.getId(), crossoverIndividuals));
            }
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        // Generate offsprings
        TupleTag<KV<String, String>> idIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, BaseItem>> baseItemIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> individualIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, CrossoverIndividuals<GenomeT>>> crossoverIndexTupleTag = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new ModelRingFn<>(idIndexTupleTag, baseItemIndexTupleTag, individualIndexTupleTag))
                .withOutputTags(crossoverIndexTupleTag, TupleTagList.of(idIndexTupleTag)
                        .and(individualIndexTupleTag).and(baseItemIndexTupleTag)));
        PCollection<KV<String, Individuals<GenomeT>>> offspringPCollection = result.get(crossoverIndexTupleTag)
                .apply(crossoverTransform)
                .apply(mutateTransform);

        // Merge the parent, evaluate, sort, and pick an individual
        TupleTag<Individuals<GenomeT>> parentTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> offspringTupleTag = new TupleTag<>();
        PCollection<KV<String, Individuals<GenomeT>>> chosenPCollection = KeyedPCollectionTuple.of(parentTupleTag, result.get(individualIndexTupleTag))
                .and(offspringTupleTag, offspringPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new UniteFamilyFn<>(parentTupleTag, offspringTupleTag)))
                .apply(Evaluate.as(fitnessTransform))
                .apply(ParDo.of(new Individuals2SelectIndividualsFn<>(1)))
                .apply(selectTransform);

        // Merge the individuals back together
        TupleTag<String> idTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> chosenTupleTag = new TupleTag<>();
        PCollection<KV<String, List<Individual<GenomeT>>>> individualsPCollection = KeyedPCollectionTuple.of(idTupleTag, result.get(idIndexTupleTag))
                .and(chosenTupleTag, chosenPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new MergeIndividualsFn<>(idTupleTag, chosenTupleTag)));

        // Wrap it back into individuals
        TupleTag<BaseItem> baseItemTupleTag = new TupleTag<>();
        TupleTag<List<Individual<GenomeT>>> individualsTupleTag = new TupleTag<>();
        return KeyedPCollectionTuple.of(baseItemTupleTag, result.get(baseItemIndexTupleTag))
                .and(individualsTupleTag, individualsPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new CreateIndividualsFn<>(baseItemTupleTag, individualsTupleTag)));
    }
}
