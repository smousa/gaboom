package org.omegabyte.gaboom.transforms;

import org.apache.beam.runners.dataflow.repackaged.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Mutate {

    public abstract static class MutateFn<GenomeT> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, Individuals<GenomeT>>> {
        private double mutRate = 0;

        public void setMutRate(double mutRate) {
            this.mutRate = mutRate;
        }

        public abstract GenomeT mutate(ProcessContext context, Random random, GenomeT genome);

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            Individuals<GenomeT> individuals = c.element().getValue();
            Random rng = individuals.getRandomGenerator();

            Individuals<GenomeT> outputIndividuals = new Individuals<>(rng.nextLong());
            individuals.getIndividuals().forEach(ind -> {
                if (mutRate > 0 && rng.nextDouble() <= mutRate) {
                    outputIndividuals.getIndividuals().add(new Individual<GenomeT>(ind.getId(), mutate(c, rng, ind.getGenome())));
                } else {
                    outputIndividuals.getIndividuals().add(ind);
                }
            });
            c.output(KV.of(key, outputIndividuals));
        }
    }

    public static class MutateTransform<GenomeT> extends PTransform<PCollection<KV<String, Individuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
        private final MutateFn<GenomeT> fn;
        private final List<PCollectionView<?>> sideInputs;

        public MutateTransform(MutateFn<GenomeT> fn, List<PCollectionView<?>> sideInputs) {
            this.fn = fn;
            this.sideInputs = sideInputs;
        }

        public MutateTransform withSideInputs(PCollectionView... sideInputs) {
            return this.withSideInputs((Iterable) Arrays.asList(sideInputs));
        }

        public MutateTransform withSideInputs(Iterable<? extends PCollectionView<?>> sideInputs) {
            List list = ImmutableList.builder().addAll(this.sideInputs).addAll(sideInputs).build();
            return new MutateTransform<>(fn, list);
        }

        public MutateTransform<GenomeT> withMutRate(double mutRate) {
            MutateTransform mt = new MutateTransform<>(fn, sideInputs);
            mt.fn.setMutRate(mutRate);
            return mt;
        }

        @Override
        public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
            return input.apply(ParDo.of(fn).withSideInputs(sideInputs));
        }
    }

    public static <GenomeT> MutateTransform<GenomeT> as(MutateFn<GenomeT> fn) {
        return new MutateTransform<>(fn, Collections.emptyList());
    }
}