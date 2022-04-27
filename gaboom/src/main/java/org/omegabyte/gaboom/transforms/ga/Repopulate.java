package org.omegabyte.gaboom.transforms.ga;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.Population;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.halloffame.HallOfFameTransform;

import java.io.Serializable;
import java.util.List;

class Repopulate<GenomeT extends Serializable> extends PTransform<PCollection<KV<String, Individuals<GenomeT>>>, PCollectionTuple> {
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;
    private final PCollection<Population<GenomeT>> population;
    private final PCollection<List<Individual<GenomeT>>> hallOfFame;
    private final TupleTag<Population<GenomeT>> populationTT;
    private final TupleTag<List<Individual<GenomeT>>> hallOfFameTT;
    private final int numBest;

    public Repopulate(Evaluate.EvaluateTransform<GenomeT> evaluateTransform, PCollection<Population<GenomeT>> population, PCollection<List<Individual<GenomeT>>> hallOfFame, TupleTag<Population<GenomeT>> populationTT, TupleTag<List<Individual<GenomeT>>> hallOfFameTT, int numBest) {
        this.evaluateTransform = evaluateTransform;
        this.population = population;
        this.hallOfFame = hallOfFame;
        this.populationTT = populationTT;
        this.hallOfFameTT = hallOfFameTT;
        this.numBest = numBest;
    }

    static class IndexPopulation<GenomeT extends Serializable> extends DoFn<Population<GenomeT>, KV<String, Population<GenomeT>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(KV.of(c.element().getId(), c.element()));
        }
    }

    static class GetIndividuals<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, List<Individual<GenomeT>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getValue().getIndividuals());
        }
    }

    static class RepopulateFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, Population<GenomeT>> {
        private final TupleTag<Population<GenomeT>> populationTT;
        private final TupleTag<Individuals<GenomeT>> individualsTT;

        public RepopulateFn(TupleTag<Population<GenomeT>> populationTT, TupleTag<Individuals<GenomeT>> individualsTT) {
            this.populationTT = populationTT;
            this.individualsTT = individualsTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            CoGbkResult result = c.element().getValue();
            Population<GenomeT> population = result.getOnly(populationTT, new Population<>(c.element().getKey()));
            Individuals<GenomeT> individuals = result.getOnly(individualsTT);

            if (!population.isNew()) {
                population = population.clone();
            }
            population.update(individuals);
            c.output(population);
        }
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        PCollection<KV<String, Individuals<GenomeT>>> individualsAtKey = input
                .apply(evaluateTransform);

        PCollection<KV<String, Population<GenomeT>>> populationAtKey = population
                .apply(ParDo.of(new IndexPopulation<>()));

        PCollection<List<Individual<GenomeT>>> hallOfFame = individualsAtKey
                .apply(ParDo.of(new GetIndividuals<>()))
                .apply(new HallOfFameTransform<>(this.hallOfFame, numBest));

        TupleTag<Population<GenomeT>> populationTT = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> individualsTT = new TupleTag<>();
        PCollection<Population<GenomeT>> population = KeyedPCollectionTuple
                .of(populationTT, populationAtKey)
                .and(individualsTT, individualsAtKey)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new RepopulateFn<>(populationTT, individualsTT)));
        return PCollectionTuple.of(this.populationTT, population).and(this.hallOfFameTT, hallOfFame);
    }
}

