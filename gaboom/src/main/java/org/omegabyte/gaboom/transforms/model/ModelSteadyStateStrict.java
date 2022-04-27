package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;
import org.omegabyte.gaboom.transforms.select.SelectEliteFn;

import java.util.List;

public class ModelSteadyStateStrict<GenomeT> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFn;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;

    public ModelSteadyStateStrict(Select.SelectFn<GenomeT> selectFn, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform) {
        this.selectFn = selectFn;
        this.crossoverTransform = crossoverTransform;
        this.mutateTransform = mutateTransform;
        this.evaluateTransform = Evaluate.as(fitnessTransform);
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

        // Merge parents with children, evaluate, and pick the best
        PCollection<KV<String, Individuals<GenomeT>>> selectedIndividuals = PCollectionList.of(result.get(selectedIndividualsAtKeyTT)).and(offspring)
                .apply(new AppendToPopulationTransform<>())
                .apply(evaluateTransform)
                .apply(ParDo.of(new IndividualsToSelectorFn<>(2)))
                .apply(Select.as(new SelectEliteFn<>()));

        // Replace selected individuals with new selection
        TupleTag<Individuals<GenomeT>> originalPopulationTT = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> selectedIndividualsTT = new TupleTag<>();
        TupleTag<List<Integer>> selectedIndexesTT = new TupleTag<>();
        return KeyedPCollectionTuple.of(originalPopulationTT, input)
                .and(selectedIndividualsTT, selectedIndividuals)
                .and(selectedIndexesTT, result.get(selectedIndexesAtKeyTT))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReplacePopulationAtIndexesFn<>(originalPopulationTT, selectedIndividualsTT, selectedIndexesTT)));
    }
}