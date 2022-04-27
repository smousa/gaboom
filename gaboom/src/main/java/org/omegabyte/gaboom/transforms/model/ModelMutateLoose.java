package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;

import java.io.Serializable;
import java.util.List;

public class ModelMutateLoose<GenomeT extends Serializable> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFn;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final int numChosen;

    public ModelMutateLoose(Select.SelectFn<GenomeT> selectFn, Mutate.MutateTransform<GenomeT> mutateTransform, int numChosen) {
        this.selectFn = selectFn;
        this.mutateTransform = mutateTransform.withMutRate(1);
        this.numChosen = numChosen;
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        // Select individuals to mutate
        TupleTag<KV<String, List<Integer>>> selectedIndexesAtKeyTT = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> selectedIndividualsAtKeyTT = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new IndividualsToSelectorFn<>(numChosen)))
                .apply(Select.as(selectFn, selectedIndexesAtKeyTT, selectedIndividualsAtKeyTT));

        // Apply mutation
        PCollection<KV<String, Individuals<GenomeT>>> mutants = result.get(selectedIndividualsAtKeyTT).apply(mutateTransform);

        // Replace selected individuals with mutants
        return input.apply(ReplacePopulationTransform.of(mutants, result.get(selectedIndexesAtKeyTT)));
    }
}
