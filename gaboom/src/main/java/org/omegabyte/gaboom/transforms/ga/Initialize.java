package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.Population;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Populate;
import org.omegabyte.gaboom.utils.IdGenerator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Initialize<GenomeT extends Serializable> extends PTransform<PBegin, PCollectionTuple> {
    private final Populate.PopulateTransform<GenomeT> populateTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;

    private final TupleTag<Population<GenomeT>> populationTT;
    private final TupleTag<List<Individual<GenomeT>>> hallOfFameTT;

    private final long seed;
    private final int numPops;
    private final int numBest;

    public Initialize(Populate.PopulateTransform<GenomeT> populateTransform, Evaluate.EvaluateTransform<GenomeT> evaluateTransform, TupleTag<Population<GenomeT>> populationTT, TupleTag<List<Individual<GenomeT>>> hallOfFameTT, long seed, int numPops, int numBest) {
        this.populateTransform = populateTransform;
        this.evaluateTransform = evaluateTransform;
        this.populationTT = populationTT;
        this.hallOfFameTT = hallOfFameTT;
        this.seed = seed;
        this.numPops = numPops;
        this.numBest = numBest;
    }

    static class InitializeFn<GenomeT extends Serializable> extends DoFn<Individuals<GenomeT>, KV<String, Individuals<GenomeT>>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            Random rng = c.element().getRandomGenerator();
            String id = IdGenerator.newId(rng, 3);
            Individuals<GenomeT> individuals = new Individuals<>(rng.nextLong(), c.element().getIndividuals());
            c.output(KV.of(id, individuals));
        }
    }

    @Override
    public PCollectionTuple expand(PBegin pBegin) {
        Random rng = new Random();
        rng.setSeed(seed);

        List<BaseItem> baseItems = new ArrayList<>();
        for (int i = 0; i < numPops; i++) {
            baseItems.add(new BaseItem(rng.nextLong()));
        }

        PCollection<Population<GenomeT>> dummyPopulation = pBegin.apply("DummyPopulation", Create.of(new Population<>("dummy")));
        PCollection<Individual<GenomeT>> dummyIndividual = pBegin.apply("DummyIndividual", Create.of(new Individual<>("dummy", null)));

        PCollection<Population<GenomeT>> population = pBegin.apply("InitialPopulation", Create.empty(dummyPopulation.getCoder()));
        PCollection<List<Individual<GenomeT>>> hallOfFame = pBegin.apply("InitialHallOfFame", Create.empty(ListCoder.of(dummyIndividual.getCoder())));

        return pBegin.apply(Create.of(baseItems))
                .apply(populateTransform)
                .apply(ParDo.of(new InitializeFn<>()))
                .apply(new Repopulate<>(evaluateTransform, population, hallOfFame, populationTT, hallOfFameTT, numBest));
    }
}

