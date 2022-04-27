package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;

import java.io.Serializable;

public class ModelDownToSize<GenomeT extends Serializable> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFnA;
    private final Select.SelectFn<GenomeT> selectFnB;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;
    private final int numOffsprings;

    public ModelDownToSize(Select.SelectFn<GenomeT> selectFnA, Select.SelectFn<GenomeT> selectFnB, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform, int numOffsprings) {
        this.selectFnA = selectFnA;
        this.selectFnB = selectFnB;
        this.crossoverTransform = crossoverTransform;
        this.mutateTransform = mutateTransform;
        this.evaluateTransform = Evaluate.as(fitnessTransform);
        this.numOffsprings = numOffsprings;
    }

    static class GetPopulationSizeFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(KV.of(c.element().getKey(), c.element().getValue().getIndividuals().size()));
        }
    }

    static class IndividualsWithSizeToSelectorFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, SelectIndividuals<GenomeT>>> {
        private final TupleTag<Individuals<GenomeT>> populationTT;
        private final TupleTag<Integer> nTT;

        public IndividualsWithSizeToSelectorFn(TupleTag<Individuals<GenomeT>> populationTT, TupleTag<Integer> nTT) {
            this.populationTT = populationTT;
            this.nTT = nTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            CoGbkResult result = c.element().getValue();

            Individuals<GenomeT> individuals = result.getOnly(populationTT);
            int n = result.getOnly(nTT);
            c.output(KV.of(key, new SelectIndividuals<>(individuals, n)));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        // Get the size of the population
        PCollection<KV<String, Integer>> populationSize = input.apply(ParDo.of(new GetPopulationSizeFn<>()));

        // Make offsprings from the initial population
        PCollection<KV<String, Individuals<GenomeT>>> offsprings = input.apply(ParDo.of(new IndividualsToSelectorFn<>(numOffsprings)))
                .apply(new GenerateOffspringsTransform<>(selectFnA, crossoverTransform, mutateTransform));

        // Append offsprings back into the initial population and evaluate
        PCollection<KV<String, Individuals<GenomeT>>> cumulativePopulation = PCollectionList.of(input).and(offsprings)
                .apply(new AppendToPopulationTransform<>())
                .apply(evaluateTransform);

        // Reduce the population back to its original size
        TupleTag<Individuals<GenomeT>> cumulativePopulationTT = new TupleTag<>();
        TupleTag<Integer> populationSizeTT = new TupleTag<>();
        return KeyedPCollectionTuple.of(cumulativePopulationTT, cumulativePopulation)
                .and(populationSizeTT, populationSize)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new IndividualsWithSizeToSelectorFn<>(cumulativePopulationTT, populationSizeTT)))
                .apply(Select.as(selectFnB));
    }
}