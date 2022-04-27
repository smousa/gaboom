package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;
import org.omegabyte.gaboom.transforms.utils.GenerateOffspringsTransform;
import org.omegabyte.gaboom.transforms.utils.Individuals2SelectIndividualsFn;
import org.omegabyte.gaboom.transforms.utils.UniteFamilyFn;

public class ModelDownToSize<GenomeT> extends ModelTransform<GenomeT> {
    private final Select.SelectFn<GenomeT> selectFnA;
    private final Select.SelectFn<GenomeT> selectFnB;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.FitnessTransform<GenomeT> fitnessTransform;
    private final int numOffsprings;

    public ModelDownToSize(Select.SelectFn<GenomeT> selectFnA, Select.SelectFn<GenomeT> selectFnB, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform, int numOffsprings) {
        this.selectFnA = selectFnA;
        this.selectFnB = selectFnB;
        this.crossoverTransform = crossoverTransform;
        this.mutateTransform = mutateTransform;
        this.fitnessTransform = fitnessTransform;
        this.numOffsprings = numOffsprings;
    }

    static class ModelDownToSizeFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, SelectIndividuals<GenomeT>>> {
        private final TupleTag<Integer> popSizeTupleTag;
        private final TupleTag<Individuals<GenomeT>> individualsTupleTag;

        public ModelDownToSizeFn(TupleTag<Integer> popSizeTupleTag, TupleTag<Individuals<GenomeT>> individualsTupleTag) {
            this.popSizeTupleTag = popSizeTupleTag;
            this.individualsTupleTag = individualsTupleTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            CoGbkResult result = c.element().getValue();

            int popSize = result.getOnly(popSizeTupleTag);
            Individuals<GenomeT> individuals = result.getOnly(individualsTupleTag);
            c.output(KV.of(key, new SelectIndividuals<>(individuals, popSize)));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {

        // Make offsprings from the initial population
        TupleTag<KV<String, Integer>> sizeIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, SelectIndividuals<GenomeT>>> selectedIndividualsTupleTag = new TupleTag<>();

        PCollectionTuple result = input.apply(ParDo.of(new Individuals2SelectIndividualsFn<GenomeT>(sizeIndexTupleTag, numOffsprings))
                .withOutputTags(selectedIndividualsTupleTag, TupleTagList.of(sizeIndexTupleTag)));

        PCollection<KV<String, Individuals<GenomeT>>> offspringsPCollection = result.get(selectedIndividualsTupleTag)
                .apply(new GenerateOffspringsTransform<>(selectFnA, crossoverTransform, mutateTransform));

        // Merge offsprings back to initial population, utils and sort
        TupleTag<Individuals<GenomeT>> firstGenTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> nextGenTupleTag = new TupleTag<>();
        PCollection<KV<String, Individuals<GenomeT>>> mergedPopulationPCollection = KeyedPCollectionTuple.of(firstGenTupleTag, input)
                .and(nextGenTupleTag, offspringsPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new UniteFamilyFn<>(firstGenTupleTag, nextGenTupleTag)))
                .apply(Evaluate.as(fitnessTransform));

        // Reduce the population back to its original size
        TupleTag<Integer> popSizeTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> individualsTupleTag = new TupleTag<>();
        return KeyedPCollectionTuple.of(popSizeTupleTag, result.get(sizeIndexTupleTag))
                .and(individualsTupleTag, mergedPopulationPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ModelDownToSizeFn<>(popSizeTupleTag, individualsTupleTag)))
                .apply(Select.as(selectFnB));
    }
}