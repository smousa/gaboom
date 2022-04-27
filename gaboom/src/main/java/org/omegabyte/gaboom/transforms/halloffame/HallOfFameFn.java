package org.omegabyte.gaboom.transforms.halloffame;

import org.apache.beam.sdk.transforms.Combine;
import org.omegabyte.gaboom.Individual;

import java.util.ArrayList;
import java.util.List;

public class HallOfFameFn<GenomeT> extends Combine.CombineFn<List<Individual<GenomeT>>, List<Individual<GenomeT>>, List<Individual<GenomeT>>> {
    final private int nBest;

    public HallOfFameFn(int nBest) {
        this.nBest = nBest;
    }

    @Override
    public List<Individual<GenomeT>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Individual<GenomeT>> addInput(List<Individual<GenomeT>> a, List<Individual<GenomeT>> b) {
        List<Individual<GenomeT>> result = new ArrayList<>();

        int aIndex = 0;
        int bIndex = 0;

        while (result.size()< nBest && aIndex < a.size() && bIndex < b.size()) {
            if (a.get(aIndex).getFitness() < b.get(bIndex).getFitness()) {
                result.add(a.get(aIndex++));
            } else {
                result.add(b.get(bIndex++));
            }
        }
        if (result.size() < nBest) {
            if (aIndex < a.size()) {
                int offset = Math.min(nBest - result.size(), a.size() - aIndex);
                result.addAll(a.subList(aIndex, aIndex+offset));
            }
            if (bIndex < b.size()) {
                int offset = Math.min(nBest-result.size(), b.size() - bIndex);
                result.addAll(b.subList(bIndex, bIndex+offset));
            }
        }
        return result;
    }

    @Override
    public List<Individual<GenomeT>> mergeAccumulators(Iterable<List<Individual<GenomeT>>> iterable) {
        List<Individual<GenomeT>> result = new ArrayList<>();
        for (List<Individual<GenomeT>> ind: iterable) {
            result = addInput(result, ind);
        }
        return result;
    }

    @Override
    public List<Individual<GenomeT>> extractOutput(List<Individual<GenomeT>> individuals) {
        return individuals;
    }
}