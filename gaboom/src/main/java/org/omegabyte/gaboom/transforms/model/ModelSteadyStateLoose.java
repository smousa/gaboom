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
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;

import java.util.List;

public class ModelSteadyStateLoose<GenomeT> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFn;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;

    public ModelSteadyStateLoose(Select.SelectFn<GenomeT> selectFn, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform) {
        this.selectFn = selectFn;
        this.crossoverTransform = crossoverTransform;
        this.mutateTransform = mutateTransform;
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        // Select individuals to crossover
        TupleTag<KV<String, List<Integer>>> selectedIndexesAtKeyTT = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> selectedIndividualsAtKeyTT = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new IndividualsToSelectorFn<>(2)))
                .apply(Select.as(selectFn, selectedIndexesAtKeyTT, selectedIndividualsAtKeyTT));

        // Produce offspring
        PCollection<KV<String, Individuals<GenomeT>>> offspring = result.get(selectedIndividualsAtKeyTT)
                .apply(ParDo.of(new IndividualsToCrossoverFn<>()))
                .apply(crossoverTransform)
                .apply(mutateTransform);

        // Replace selected individuals with offspring
        TupleTag<Individuals<GenomeT>> originalPopulationTT = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> offspringTT = new TupleTag<>();
        TupleTag<List<Integer>> selectedIndexesTT = new TupleTag<>();
        return KeyedPCollectionTuple.of(originalPopulationTT, input)
                .and(offspringTT, offspring)
                .and(selectedIndexesTT, result.get(selectedIndexesAtKeyTT))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReplacePopulationAtIndexesFn<>(originalPopulationTT, offspringTT, selectedIndexesTT)));
    }
}