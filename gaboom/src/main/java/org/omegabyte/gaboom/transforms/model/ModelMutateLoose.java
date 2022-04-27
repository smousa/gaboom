package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;
import org.omegabyte.gaboom.transforms.utils.Individuals2SelectIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.ReplaceIndividualsFn;

import java.util.List;

public class ModelMutateLoose<GenomeT> extends ModelTransform<GenomeT> {
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
        //Select individuals
        TupleTag<KV<String, List<Integer>>> selectIndicesTupleTag = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> selectIndividualsTupleTag = new TupleTag<>();
        PCollectionTuple result = input
                .apply(ParDo.of(new Individuals2SelectIndividualsFn<>(numChosen)))
                .apply(Select.as(selectFn, selectIndicesTupleTag, selectIndividualsTupleTag));

        // Apply mutation
        PCollection<KV<String, Individuals<GenomeT>>> mutants = result.get(selectIndividualsTupleTag)
                .apply(mutateTransform);

        // Integrate the new members into the population
        TupleTag<Individuals<GenomeT>> firstGenTupleTag = new TupleTag<>();
        TupleTag<List<Integer>> indicesTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> offspringTupleTag = new TupleTag<>();
        return KeyedPCollectionTuple.of(firstGenTupleTag, input)
                .and(indicesTupleTag, result.get(selectIndicesTupleTag))
                .and(offspringTupleTag, mutants)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ReplaceIndividualsFn<>(firstGenTupleTag, offspringTupleTag, indicesTupleTag)));
    }
}
