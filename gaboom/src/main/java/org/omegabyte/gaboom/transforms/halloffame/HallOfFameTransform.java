package org.omegabyte.gaboom.transforms.halloffame;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.omegabyte.gaboom.Individual;

import java.util.List;

public class HallOfFameTransform<GenomeT> extends PTransform<PCollection<List<Individual<GenomeT>>>, PCollection<List<Individual<GenomeT>>>> {
    private final PCollection<List<Individual<GenomeT>>> hallOfFamePCollection;
    private final int nBest;

    public HallOfFameTransform(PCollection<List<Individual<GenomeT>>> hallOfFamePCollection, int nBest) {
        this.hallOfFamePCollection = hallOfFamePCollection;
        this.nBest = nBest;
    }

    @Override
    public PCollection<List<Individual<GenomeT>>> expand(PCollection<List<Individual<GenomeT>>> input) {
        return PCollectionList.of(hallOfFamePCollection)
                .and(input)
                .apply(Flatten.pCollections())
                .apply(Combine.globally(new HallOfFameFn<>(nBest)));
    }
}