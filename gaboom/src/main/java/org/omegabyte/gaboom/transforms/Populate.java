package org.omegabyte.gaboom.transforms;

import org.apache.beam.runners.dataflow.repackaged.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Populate {

    public abstract static class PopulateFn<GenomeT extends Serializable> extends DoFn<BaseItem, Individuals<GenomeT>> {
        private int popSize = 1;

        public void setPopSize(int popSize) {
            this.popSize = popSize;
        }

        public abstract GenomeT makeGenome(ProcessContext context, Random random);

        @ProcessElement
        public void processElement(ProcessContext c) {
            BaseItem baseItem = c.element();
            Random rng = baseItem.getRandomGenerator();

            Individuals<GenomeT> individuals = new Individuals<>(baseItem.getSeed());
            for (int i = 0; i < popSize; i++) {
                individuals.getIndividuals().add(new Individual<>(rng, makeGenome(c, rng)));
            }
            c.output(individuals);
        }
    }

    public static class PopulateTransform<GenomeT extends Serializable> extends PTransform<PCollection<BaseItem>, PCollection<Individuals<GenomeT>>> {
        private final PopulateFn<GenomeT> fn;
        private final List<PCollectionView<?>> sideInputs;

        public PopulateTransform(PopulateFn<GenomeT> fn, List<PCollectionView<?>> sideInputs) {
            this.fn = SerializableUtils.clone(fn);
            this.sideInputs = sideInputs;
        }

        public PopulateTransform<GenomeT> withSideInputs(PCollectionView... sideInputs) {
            return this.withSideInputs((Iterable) Arrays.asList(sideInputs));
        }

        public PopulateTransform<GenomeT> withSideInputs(Iterable<? extends PCollectionView<?>> sideInputs) {
            List list = ImmutableList.builder().addAll(this.sideInputs).addAll(sideInputs).build();
            return new PopulateTransform<>(fn, list);
        }

        public PopulateTransform<GenomeT> withPopSize(int popSize) {
            PopulateTransform pt = new PopulateTransform<>(fn, sideInputs);
            pt.fn.setPopSize(popSize);
            return pt;
        }

        @Override
        public PCollection<Individuals<GenomeT>> expand(PCollection<BaseItem> input) {
            return input.apply(ParDo.of(fn).withSideInputs(sideInputs));
        }
    }

    public static <GenomeT extends Serializable> PopulateTransform<GenomeT> as(PopulateFn<GenomeT> fn) {
        return new PopulateTransform<>(fn, Collections.emptyList());
    }
}