package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;
import org.omegabyte.gaboom.transforms.select.SelectEliteFn;
import org.omegabyte.gaboom.transforms.utils.Individuals2CrossoverIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.Individuals2SelectIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.ReplaceIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.UniteFamilyFn;

import java.util.List;

public class ModelSteadyStateStrict<GenomeT> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFn;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.FitnessTransform<GenomeT> fitnessTransform;

    public ModelSteadyStateStrict(Select.SelectFn<GenomeT> selectFn, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform) {
        this.selectFn = selectFn;
        this.crossoverTransform = crossoverTransform;
        this.mutateTransform = mutateTransform;
        this.fitnessTransform = fitnessTransform;
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        // Select a pair
        TupleTag<KV<String, List<Integer>>> selectIndicesTupleTag = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> selectIndividualsTupleTag = new TupleTag<>();
        PCollectionTuple result = input
                .apply(ParDo.of(new Individuals2SelectIndividualsFn<>(2)))
                .apply(Select.as(selectFn, selectIndicesTupleTag, selectIndividualsTupleTag));

        // Produce offspring
        PCollection<KV<String, Individuals<GenomeT>>> offspringPCollection = result.get(selectIndividualsTupleTag)
                .apply(ParDo.of(new Individuals2CrossoverIndividualsFn<>()))
                .apply(crossoverTransform)
                .apply(mutateTransform);

        // Merge parents and children, sort, and pick the best
        TupleTag<Individuals<GenomeT>> parentsTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> offspringTupleTag = new TupleTag<>();
        PCollection<KV<String, Individuals<GenomeT>>> newMembersPCollection = KeyedPCollectionTuple
                .of(parentsTupleTag, result.get(selectIndividualsTupleTag))
                .and(offspringTupleTag, offspringPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new UniteFamilyFn<>(parentsTupleTag, offspringTupleTag)))
                .apply(Evaluate.as(fitnessTransform))
                .apply(ParDo.of(new Individuals2SelectIndividualsFn<>(2)))
                .apply(Select.as(new SelectEliteFn<>()));

        // Integrate back into population
        TupleTag<Individuals<GenomeT>> inputTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> newMembersTupleTag = new TupleTag<>();
        TupleTag<List<Integer>> indicesTupleTag = new TupleTag<>();
        return KeyedPCollectionTuple.of(inputTupleTag, input)
                .and(newMembersTupleTag, newMembersPCollection)
                .and(indicesTupleTag, result.get(selectIndicesTupleTag))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReplaceIndividualsFn<>(inputTupleTag, newMembersTupleTag, indicesTupleTag)));
    }
}