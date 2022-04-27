package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ModelMutateStrict<GenomeT> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFn;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;
    private final int numChosen;

    public ModelMutateStrict(Select.SelectFn<GenomeT> selectFn, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform, int numChosen) {
        this.selectFn = selectFn;
        this.mutateTransform = mutateTransform.withMutRate(1);
        this.evaluateTransform = Evaluate.as(fitnessTransform);
        this.numChosen = numChosen;
    }

    static class DedupeIndividualsFn<GenomeT> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, Individuals<GenomeT>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            Individuals<GenomeT> individuals = c.element().getValue();

            List<Individual<GenomeT>> individualList = new ArrayList<>();
            Set<String> ids = new HashSet<>();
            individuals.getIndividuals().forEach(ind -> {
                if (!ids.contains(ind.getId())) {
                    individualList.add(ind);
                    ids.add(ind.getId());
                }
            });
            c.output(KV.of(key, new Individuals<>(individuals.getSeed(), individualList)));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        // Select individuals to mutate
        TupleTag<KV<String, List<Integer>>> selectedIndexesAtKeyTT = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> selectedIndividualsAtKeyTT = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new IndividualsToSelectorFn<>(numChosen)))
                .apply(Select.as(selectFn, selectedIndexesAtKeyTT, selectedIndividualsAtKeyTT));

        // Apply mutation
        PCollection<KV<String, Individuals<GenomeT>>> mutantsAtKey = result.get(selectedIndividualsAtKeyTT)
                .apply(mutateTransform);

        // Append mutants to originals, evaluate, extract the best members
        PCollection<KV<String, Individuals<GenomeT>>> bestSelection = PCollectionList.of(result.get(selectedIndividualsAtKeyTT)).and(mutantsAtKey)
                .apply(new AppendToPopulationTransform<>())
                .apply(evaluateTransform)
                .apply(ParDo.of(new DedupeIndividualsFn<>()));

        // Replace selected individuals with mutants
        TupleTag<Individuals<GenomeT>> originalPopulationTT = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> bestSelectionTT = new TupleTag<>();
        TupleTag<List<Integer>> selectedIndexesTT = new TupleTag<>();
        return KeyedPCollectionTuple.of(originalPopulationTT, input)
                .and(bestSelectionTT, bestSelection)
                .and(selectedIndexesTT, result.get(selectedIndexesAtKeyTT))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReplacePopulationAtIndexesFn<>(originalPopulationTT, bestSelectionTT, selectedIndexesTT)));
    }
}