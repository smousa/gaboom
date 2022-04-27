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

import java.util.Random;

public class ModelSimAnn<GenomeT> extends ModelTransform<GenomeT> {
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

    static class ModelSimAnnFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individual<GenomeT>>> {
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
        PCollection<KV<String, Individual<GenomeT>>> selectedIndividual = KeyedPCollectionTuple
                .of(originalIndividualTT, result.get(originalIndividualAtIdTT))
                .and(mutantTT, mutant)
                .and(keyTT, result.get(keyAtIdTT))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ModelSimAnnFn<>(originalIndividualTT, mutantTT, keyTT, t)));

        // Get the new population and do it again
        TupleTag<Individual<GenomeT>> selectedIndividualTT = new TupleTag<>();
        TupleTag<BaseItem> baseItemTT = new TupleTag<>();

        return KeyedPCollectionTuple.of(selectedIndividualTT, selectedIndividual)
                .and(baseItemTT, result.get(baseItemAtKeyTT))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new IndividualsFromIndividualFn<>(selectedIndividualTT, baseItemTT)))
                .apply(new ModelSimAnn<>(mutateTransform, evaluateTransform, t * alpha, tmin, alpha));
    }
}