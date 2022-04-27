package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;

import java.io.Serializable;
import java.util.Random;

public class ModelSimAnn<GenomeT extends Serializable> extends ModelTransform<GenomeT> {
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.EvaluateTransform<GenomeT> evaluateTransform;
    private final double t;
    private final double tmin;
    private final double alpha;

    public ModelSimAnn(Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform, double t, double tmin, double alpha) {
        this.mutateTransform = mutateTransform.withMutRate(1);
        this.evaluateTransform = Evaluate.as(fitnessTransform);
        this.t = t;
        this.tmin = tmin;
        this.alpha = alpha;
    }

    public ModelSimAnn(Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.EvaluateTransform<GenomeT> evaluateTransform, double t, double tmin, double alpha) {
        this.mutateTransform = mutateTransform.withMutRate(1);
        this.evaluateTransform = evaluateTransform;
        this.t = t;
        this.tmin = tmin;
        this.alpha = alpha;
    }

    static class SplitIndividualsFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, Individuals<GenomeT>>> {
        private final TupleTag<KV<String, String>> keyAtIdTT;
        private final TupleTag<KV<String, BaseItem>> baseItemAtKeyTT;

        public SplitIndividualsFn(TupleTag<KV<String, String>> keyAtIdTT, TupleTag<KV<String, BaseItem>> baseItemAtKeyTT) {
            this.keyAtIdTT = keyAtIdTT;
            this.baseItemAtKeyTT = baseItemAtKeyTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            Individuals<GenomeT> individuals = c.element().getValue();
            Random rng = individuals.getRandomGenerator();

            c.output(baseItemAtKeyTT, KV.of(key, new BaseItem(rng.nextLong())));
            individuals.getIndividuals().forEach(ind -> {
                c.output(keyAtIdTT, KV.of(ind.getId(), key));

                Individuals<GenomeT> result = new Individuals<>(rng.nextLong());
                result.getIndividuals().add(ind);
                c.output(KV.of(ind.getId(), result));
            });
        }
    }

    static class ModelSimAnnFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individual<GenomeT>>> {
        private final TupleTag<Individuals<GenomeT>> firstGenTupleTag;
        private final TupleTag<Individuals<GenomeT>> nextGenTupleTag;
        private final TupleTag<String> idTupleTag;
        private final double t;

        public ModelSimAnnFn(TupleTag<Individuals<GenomeT>> firstGenTupleTag, TupleTag<Individuals<GenomeT>> nextGenTupleTag, TupleTag<String> idTupleTag, double t) {
            this.firstGenTupleTag = firstGenTupleTag;
            this.nextGenTupleTag = nextGenTupleTag;
            this.idTupleTag = idTupleTag;
            this.t = t;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            CoGbkResult result = c.element().getValue();
            String key = result.getOnly(idTupleTag);

            Individual<GenomeT> firstGen = result.getOnly(firstGenTupleTag).getIndividuals().get(0);
            Individual<GenomeT> nextGen = result.getOnly(nextGenTupleTag).getIndividuals().get(0);
            Random rng = result.getOnly(nextGenTupleTag).getRandomGenerator();

            if (nextGen.getFitness() < firstGen.getFitness()) {
                c.output(KV.of(key, nextGen));
                return;
            }
            double p = Math.exp(firstGen.getFitness() - nextGen.getFitness() / t);
            if (p > rng.nextDouble()) {
                c.output(KV.of(key, nextGen));
                return;
            }
            c.output(KV.of(key, nextGen));
        }
    }


    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, Individuals<GenomeT>>> input) {
        if (t < tmin) {
            return input;
        }

        // Set up indexes for each individual
        TupleTag<KV<String, String>> keyAtIdTT = new TupleTag<>();
        TupleTag<KV<String, BaseItem>> baseItemAtKeyTT = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> originalIndividualAtIdTT = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new SplitIndividualsFn<GenomeT>(keyAtIdTT, baseItemAtKeyTT))
                .withOutputTags(originalIndividualAtIdTT,
                        TupleTagList.of(keyAtIdTT).and(baseItemAtKeyTT)));

        // Apply mutation and evaluate
        PCollection<KV<String, Individuals<GenomeT>>> mutant = result.get(originalIndividualAtIdTT)
                .apply(mutateTransform)
                .apply(evaluateTransform);

        // Append mutants to originals, perform simulated annealing to select members, and restore index
        TupleTag<Individuals<GenomeT>> originalIndividualTT = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> mutantTT = new TupleTag<>();
        TupleTag<String> keyTT = new TupleTag<>();
        return KeyedPCollectionTuple
                .of(originalIndividualTT, result.get(originalIndividualAtIdTT))
                .and(mutantTT, mutant)
                .and(keyTT, result.get(keyAtIdTT))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ModelSimAnnFn<>(originalIndividualTT, mutantTT, keyTT, t)))

                // Get the new population and do it again
                .apply(IndividualsFromIndividualTransform.of(result.get(baseItemAtKeyTT)))
                .apply(ParDo.of(new SetMutantNameFn<>()))
                .apply(new ModelSimAnn<>(mutateTransform, evaluateTransform, t*alpha, tmin, alpha));
    }
}