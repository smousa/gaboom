package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.Population;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.model.ModelTransform;
import org.omegabyte.gaboom.transforms.model.SetMutantNameFn;

import java.io.Serializable;
import java.util.List;

public class Evolve<GenomeT extends Serializable> extends PTransform<PCollectionTuple, PCollectionTuple> {
    private final ModelTransform<GenomeT> modelTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;
    private final TupleTag<Population<GenomeT>> populationTT;
    private final TupleTag<List<Individual<GenomeT>>> hallOfFameTT;
    private final int numBest;

    public Evolve(ModelTransform<GenomeT> modelTransform, Evaluate.EvaluateTransform<GenomeT> evaluateTransform, TupleTag<Population<GenomeT>> populationTT, TupleTag<List<Individual<GenomeT>>> hallOfFameTT, int numBest) {
        this.modelTransform = modelTransform;
        this.evaluateTransform = evaluateTransform;
        this.populationTT = populationTT;
        this.hallOfFameTT = hallOfFameTT;
        this.numBest = numBest;
    }

    static class IndexIndividuals<GenomeT extends Serializable> extends DoFn<Population<GenomeT>, KV<String, Individuals<GenomeT>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(KV.of(c.element().getId(), new Individuals<>(c.element().getSeed(), c.element().getIndividuals())));
        }
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        return input.get(populationTT)
                .apply(ParDo.of(new IndexIndividuals<>()))
                .apply(modelTransform)
                .apply(ParDo.of(new SetMutantNameFn<>()))
                .apply(new Repopulate<>(evaluateTransform, input.get(populationTT), input.get(hallOfFameTT), populationTT, hallOfFameTT, numBest));
    }
}