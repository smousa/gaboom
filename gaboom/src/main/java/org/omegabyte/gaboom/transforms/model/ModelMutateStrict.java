package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;
import org.omegabyte.gaboom.transforms.select.SelectEliteFn;
import org.omegabyte.gaboom.transforms.utils.*;

import java.util.List;

public class ModelMutateStrict<GenomeT> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFn;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.FitnessTransform<GenomeT> fitnessTransform;
    private final int numChosen;

    public ModelMutateStrict(Select.SelectFn<GenomeT> selectFn, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform, int numChosen) {
        this.selectFn = selectFn;
        this.mutateTransform = mutateTransform.withMutRate(1);
        this.fitnessTransform = fitnessTransform;
        this.numChosen = numChosen;
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        //Select individuals
        TupleTag<KV<String, List<Integer>>> selectIndicesTupleTag = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> selectIndividualsTupleTag = new TupleTag<>();
        PCollectionTuple result = input
                .apply(ParDo.of(new Individuals2SelectIndividualsFn<>(numChosen)))
                .apply(Select.as(selectFn, selectIndicesTupleTag, selectIndividualsTupleTag));

        // Split by individual
        TupleTag<KV<String, String>> idIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, BaseItem>> baseItemIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> individualsIndexTupleTag = new TupleTag<>();
        PCollectionTuple result2 = result.get(selectIndividualsTupleTag)
                .apply(ParDo.of(new SplitIndividualsFn<GenomeT>(idIndexTupleTag, baseItemIndexTupleTag))
                .withOutputTags(individualsIndexTupleTag, TupleTagList.of(idIndexTupleTag).and(baseItemIndexTupleTag)));

        // Apply mutation
        PCollection<KV<String, Individuals<GenomeT>>> mutant = result2.get(individualsIndexTupleTag)
                .apply(mutateTransform);

        // Unite with original and pick the best
        TupleTag<Individuals<GenomeT>> originalTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> mutantTupleTag = new TupleTag<>();
        PCollection<KV<String, Individuals<GenomeT>>> chosen = KeyedPCollectionTuple.of(originalTupleTag, result2.get(individualsIndexTupleTag))
                .and(mutantTupleTag, mutant)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new UniteFamilyFn<>(originalTupleTag, mutantTupleTag)))
                .apply(Evaluate.as(fitnessTransform))
                .apply(ParDo.of(new Individuals2SelectIndividualsFn<>(1)))
                .apply(Select.as(new SelectEliteFn<>()));

        // Restore the index
        TupleTag<String> idTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> chosenTupleTag = new TupleTag<>();
        PCollection<KV<String, Iterable<Individual<GenomeT>>>> groupedChosen = KeyedPCollectionTuple.of(idTupleTag, result2.get(idIndexTupleTag))
                .and(chosenTupleTag, chosen)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReIndexIndividuals<>(idTupleTag, chosenTupleTag)))
                .apply(GroupByKey.create());

        // Create the final selection
        TupleTag<Iterable<Individual<GenomeT>>> groupedChosenTupleTag = new TupleTag<>();
        TupleTag<BaseItem> baseItemTupleTag = new TupleTag<>();
        PCollection<KV<String, Individuals<GenomeT>>> newMembers = KeyedPCollectionTuple.of(baseItemTupleTag, result2.get(baseItemIndexTupleTag))
                .and(groupedChosenTupleTag, groupedChosen)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new CreateIndividualsFromIndividualFn<>(groupedChosenTupleTag, baseItemTupleTag)));

        // Merge back into the population
        TupleTag<Individuals<GenomeT>> inputTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> newMembersTupleTag = new TupleTag<>();
        TupleTag<List<Integer>> indicesTupleTag = new TupleTag<>();
        return KeyedPCollectionTuple.of(inputTupleTag, input)
                .and(newMembersTupleTag, newMembers)
                .and(indicesTupleTag, result.get(selectIndicesTupleTag))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReplaceIndividualsFn<>(inputTupleTag, newMembersTupleTag, indicesTupleTag)));
    }
}