package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.commons.text.RandomStringGenerator;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.Population;
import org.omegabyte.gaboom.transforms.ga.GetIndividualListFn;
import org.omegabyte.gaboom.transforms.ga.MapPopulationFn;
import org.omegabyte.gaboom.transforms.halloffame.HallOfFameTransform;
import org.omegabyte.gaboom.transforms.model.ModelTransform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class GA<GenomeT extends Serializable> {
    // Required fields
    private final int numPops = 1;
    private final Populate.PopulateTransform<GenomeT> populateTransform;
    private final ModelTransform<GenomeT> modelTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;

    //Optional fields
    private int numBest = 1;
    private long seed = new Date().getTime();

    // Fields generated at runtime
    private TupleTag<KV<String, Individuals<GenomeT>>> individualsIndexTupleTag = new TupleTag<>();
    private TupleTag<Population<GenomeT>> populationsTupleTag = new TupleTag<>();
    private TupleTag<List<Individual<GenomeT>>> hallOfFameTupleTag = new TupleTag<>();
    private long created;
    private int generations = 0;

    public GA(int popSize, Populate.PopulateTransform<GenomeT> populateTransform, ModelTransform<GenomeT> modelTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform) {
        this.populateTransform = populateTransform.withPopSize(popSize);
        this.modelTransform = modelTransform;
        this.evaluateTransform = Evaluate.as(fitnessTransform);
    }

    class InitializeTransform extends PTransform<PBegin, PCollectionTuple> {
        class InitializeFn extends DoFn<Individuals<GenomeT>, KV<String, Individuals<GenomeT>>> {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Random rng = c.element().getRandomGenerator();
                String id = new RandomStringGenerator.Builder()
                        .usingRandom(rng::nextInt)
                        .withinRange('0', '9')
                        .withinRange('A', 'Z')
                        .withinRange('a', 'z')
                        .build().generate(3);
                c.output(KV.of(id, new Individuals<>(rng.nextLong(), c.element().getIndividuals())));
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
            PCollection<KV<String, Individuals<GenomeT>>> individualsIndex = pBegin.apply(Create.of(baseItems))
                    .apply(populateTransform)
                    .apply(ParDo.of(new InitializeFn()));
            PCollection<Population<GenomeT>> population = pBegin.apply(Create.of(new ArrayList<>()));
            PCollection<List<Individual<GenomeT>>> hallOfFame = pBegin.apply(Create.of(new ArrayList<>()));

            individualsIndexTupleTag = new TupleTag<>();
            populationsTupleTag = new TupleTag<>();
            hallOfFameTupleTag = new TupleTag<>();
            return PCollectionTuple.of(individualsIndexTupleTag, individualsIndex)
                    .and(populationsTupleTag, population)
                    .and(hallOfFameTupleTag, hallOfFame)
                    .apply(new RepopulateTransform());
        }
    }

    class EvolveTransform extends PTransform<PCollectionTuple, PCollectionTuple> {
        class EvolveFn extends DoFn<Population<GenomeT>, KV<String, Individuals<GenomeT>>> {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(KV.of(c.element().getId(), c.element()));
            }
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple pCollectionTuple) {
            PCollection<KV<String, Individuals<GenomeT>>> individualsIndex = pCollectionTuple.get(populationsTupleTag)
                    .apply(ParDo.of(new EvolveFn()))
                    .apply(modelTransform);
            PCollection<Population<GenomeT>> population = pCollectionTuple.get(populationsTupleTag);
            PCollection<List<Individual<GenomeT>>> hallOfFame = pCollectionTuple.get(hallOfFameTupleTag);

            individualsIndexTupleTag = new TupleTag<>();
            populationsTupleTag = new TupleTag<>();
            hallOfFameTupleTag = new TupleTag<>();
            return PCollectionTuple.of(individualsIndexTupleTag, individualsIndex)
                    .and(populationsTupleTag, population)
                    .and(hallOfFameTupleTag, hallOfFame)
                    .apply(new RepopulateTransform());
        }
    }

    class RepopulateTransform extends PTransform<PCollectionTuple, PCollectionTuple> {
        class RepopulateFn extends DoFn<KV<String, CoGbkResult>, Population<GenomeT>> {
            private final TupleTag<Population<GenomeT>> populationTupleTag;
            private final TupleTag<Individuals<GenomeT>> individualsTupleTag;

            public RepopulateFn(TupleTag<Population<GenomeT>> populationTupleTag, TupleTag<Individuals<GenomeT>> individualsTupleTag) {
                this.populationTupleTag = populationTupleTag;
                this.individualsTupleTag = individualsTupleTag;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                String key = c.element().getKey();
                CoGbkResult result = c.element().getValue();

                Population<GenomeT> population = result.getOnly(populationTupleTag, new Population<>(key));
                Individuals<GenomeT> individuals = result.getOnly(individualsTupleTag);
                population.update(individuals);
                c.output(population);
            }
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple pCollectionTuple) {
            PCollection<KV<String, Population<GenomeT>>> populationIndex = pCollectionTuple
                    .get(populationsTupleTag)
                    .apply(ParDo.of(new MapPopulationFn<>()));

            PCollection<KV<String, Individuals<GenomeT>>> individuals = pCollectionTuple.get(individualsIndexTupleTag)
                    .apply(evaluateTransform);

            PCollection<List<Individual<GenomeT>>> hallOfFame = individuals.apply(ParDo.of(new GetIndividualListFn<>()))
                    .apply(new HallOfFameTransform<>(pCollectionTuple.get(hallOfFameTupleTag), numBest));

            TupleTag<Population<GenomeT>> populationTupleTag = new TupleTag<>();
            TupleTag<Individuals<GenomeT>> individualsTupleTag = new TupleTag<>();
            PCollection<Population<GenomeT>> population = KeyedPCollectionTuple
                    .of(populationTupleTag, populationIndex)
                    .and(individualsTupleTag, individuals)
                    .apply(CoGroupByKey.create())
                    .apply(ParDo.of(new RepopulateFn(populationTupleTag, individualsTupleTag)));
            return PCollectionTuple.of(populationsTupleTag, population).and(hallOfFameTupleTag, hallOfFame);
        }
    }

    public void setNumBest(int numBest) {
        this.numBest = numBest;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    public long getAge() {
        return new Date().getTime() - created;
    }

    public int getGenerations() {
        return generations;
    }

    public InitializeTransform initialize() {
        this.created = new Date().getTime();
        return new InitializeTransform();
    }

    public EvolveTransform evolve() {
        this.generations++;
        return new EvolveTransform();
    }
}