package org.omegabyte.gaboom.transforms.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.NBaseItem;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenerateOffspringsTransform<GenomeT> extends PTransform<PCollection<KV<String, SelectIndividuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
    private final Select.SelectNoIndexTransform<GenomeT> selectTransform;
    private final Crossover.CrossoverTransform<GenomeT> crossoverTransform;
    private final Mutate.MutateTransform<GenomeT> mutateTransform;

    public GenerateOffspringsTransform(Select.SelectFn<GenomeT> selectFn, Crossover.CrossoverTransform<GenomeT> crossoverTransform, Mutate.MutateTransform<GenomeT> mutateTransform) {
        this.selectTransform = Select.as(selectFn);
        this.crossoverTransform = crossoverTransform;
        this.mutateTransform = mutateTransform;
    }

    static class Convert2SelectIndividualsFn<GenomeT> extends DoFn<KV<String, SelectIndividuals<GenomeT>>, KV<String, SelectIndividuals<GenomeT>>> {
        private final TupleTag<KV<String, NBaseItem>> nBaseItemIndexTupleTag;

        public Convert2SelectIndividualsFn(TupleTag<KV<String, NBaseItem>> nBaseItemIndexTupleTag) {
            this.nBaseItemIndexTupleTag = nBaseItemIndexTupleTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            SelectIndividuals<GenomeT> selectIndividuals = c.element().getValue();
            Random rng = selectIndividuals.getRandomGenerator();

            c.output(nBaseItemIndexTupleTag, KV.of(key, new NBaseItem(rng.nextLong(), selectIndividuals.getN())));
            for (int i = 0; i < (selectIndividuals.getN()+1)/2; i++) {
                c.output(KV.of(key, new SelectIndividuals<>(selectIndividuals, 2)));
            }
        }
    }

    static class Convert2IndividualsFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
        private static final Logger logger = LoggerFactory.getLogger(Convert2IndividualsFn.class);

        private final TupleTag<NBaseItem> nBaseItemTupleTag;
        private final TupleTag<Individuals<GenomeT>> individualsTupleTag;

        public Convert2IndividualsFn(TupleTag<NBaseItem> nBaseItemTupleTag, TupleTag<Individuals<GenomeT>> individualsTupleTag) {
            this.nBaseItemTupleTag = nBaseItemTupleTag;
            this.individualsTupleTag = individualsTupleTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            CoGbkResult result = c.element().getValue();

            NBaseItem nBaseItem = result.getOnly(nBaseItemTupleTag);
            List<Individual<GenomeT>> individuals = new ArrayList<>();
            result.getAll(individualsTupleTag).forEach(inds -> {
                individuals.addAll(inds.getIndividuals());
            });

            if (individuals.size() < nBaseItem.getN()) {
                logger.error("Not enough individuals to populate, id={}", key);
                return;
            }
            c.output(KV.of(key, new Individuals<>(nBaseItem.getSeed(), individuals.subList(0, nBaseItem.getN()))));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, SelectIndividuals<GenomeT>>> input) {
        // Set up selectors
        TupleTag<KV<String, NBaseItem>> nBaseItemIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, SelectIndividuals<GenomeT>>> selectIndividualsIndexTupleTag = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new Convert2SelectIndividualsFn<GenomeT>(nBaseItemIndexTupleTag))
                .withOutputTags(selectIndividualsIndexTupleTag, TupleTagList.of(nBaseItemIndexTupleTag)));

        // Create offspring
        PCollection<KV<String, Individuals<GenomeT>>> offspringPCollection = result.get(selectIndividualsIndexTupleTag)
                .apply(selectTransform)
                .apply(ParDo.of(new Individuals2CrossoverIndividualsFn<>()))
                .apply(crossoverTransform)
                .apply(mutateTransform);

        // Merge offspring back into the result
        TupleTag<NBaseItem> nBaseItemTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> offspringTupleTag = new TupleTag<>();
        return KeyedPCollectionTuple.of(nBaseItemTupleTag, result.get(nBaseItemIndexTupleTag))
                .and(offspringTupleTag, offspringPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new Convert2IndividualsFn<>(nBaseItemTupleTag, offspringTupleTag)));
    }
}