package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;

import java.io.Serializable;

public class ModelGenerational<GenomeT extends Serializable> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFn;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;

    public ModelGenerational(Select.SelectFn<GenomeT> selectFn, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform) {
        this.selectFn = selectFn;
        this.crossoverTransform = crossoverTransform;
        this.mutateTransform = mutateTransform;
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        return input.apply(ParDo.of(new IndividualsToSelectorFn<>()))
                .apply(new GenerateOffspringsTransform<>(selectFn, crossoverTransform, mutateTransform));
    }
}
