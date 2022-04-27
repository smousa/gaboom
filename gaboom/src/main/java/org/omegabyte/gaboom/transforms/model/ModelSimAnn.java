package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
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
import org.omegabyte.gaboom.transforms.utils.CreateIndividualsFromIndividualFn;
import org.omegabyte.gaboom.transforms.utils.SplitIndividualsFn;

import java.util.Random;

public class ModelSimAnn<GenomeT> extends ModelTransform<GenomeT> {
    private final Mutate.MutateTransform<GenomeT> mutateTransform;
    private final Evaluate.FitnessTransform<GenomeT> fitnessTransform;
    private final double t;
    private final double tmin;
    private final double alpha;

    public ModelSimAnn(Mutate.MutateTransform<GenomeT> mutateTransform, Evaluate.FitnessTransform<GenomeT> fitnessTransform, double t, double tmin, double alpha) {
        this.mutateTransform = mutateTransform.withMutRate(1);
        this.fitnessTransform = fitnessTransform;
        this.t = t;
        this.tmin = tmin;
        this.alpha = alpha;
    }

    static class ModelSimAnnFn<GenomeT> extends DoFn<KV<String, CoGbkResult>, KV<String, Individual<GenomeT>>> {
        private final TupleTag<String> idTupleTag;
        private final TupleTag<Individuals<GenomeT>> firstGenTupleTag;
        private final TupleTag<Individuals<GenomeT>> nextGenTupleTag;
        private final double t;

        public ModelSimAnnFn(TupleTag<String> idTupleTag, TupleTag<Individuals<GenomeT>> firstGenTupleTag, TupleTag<Individuals<GenomeT>> nextGenTupleTag, double t) {
            this.idTupleTag = idTupleTag;
            this.firstGenTupleTag = firstGenTupleTag;
            this.nextGenTupleTag = nextGenTupleTag;
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

        // Split by individual
        TupleTag<KV<String, String>> idIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, BaseItem>> baseItemIndexTupleTag = new TupleTag<>();
        TupleTag<KV<String, Individuals<GenomeT>>> individualsIndexTupleTag = new TupleTag<>();
        PCollectionTuple result = input.apply(ParDo.of(new SplitIndividualsFn<GenomeT>(idIndexTupleTag, baseItemIndexTupleTag))
                .withOutputTags(individualsIndexTupleTag, TupleTagList.of(idIndexTupleTag).and(baseItemIndexTupleTag)));

        // Mutate and evaluate the individuals
        PCollection<KV<String, Individuals<GenomeT>>> neighbors = result.get(individualsIndexTupleTag)
                .apply(mutateTransform)
                .apply(Evaluate.as(fitnessTransform));

        // Perform simulated annealing
        TupleTag<String> idTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> originalTupleTag = new TupleTag<>();
        TupleTag<Individuals<GenomeT>> neighborTupleTag = new TupleTag<>();
        PCollection<KV<String, Iterable<Individual<GenomeT>>>> indiPCollection = KeyedPCollectionTuple.of(originalTupleTag, result.get(individualsIndexTupleTag))
                .and(neighborTupleTag, neighbors)
                .and(idTupleTag, result.get(idIndexTupleTag))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ModelSimAnnFn<>(idTupleTag, originalTupleTag, neighborTupleTag, t)))
                .apply(GroupByKey.create());

        // Merge the individuals and do it again
        TupleTag<BaseItem> baseItemTupleTag = new TupleTag<>();
        TupleTag<Iterable<Individual<GenomeT>>> indiTupleTag = new TupleTag<>();
        return KeyedPCollectionTuple.of(baseItemTupleTag, result.get(baseItemIndexTupleTag))
                .and(indiTupleTag, indiPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new CreateIndividualsFromIndividualFn<GenomeT>(indiTupleTag, baseItemTupleTag)))
                .apply(new ModelSimAnn<>(mutateTransform, fitnessTransform, t*alpha, tmin, alpha));
    }
}
