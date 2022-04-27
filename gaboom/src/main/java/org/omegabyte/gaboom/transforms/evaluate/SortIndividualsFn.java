package org.omegabyte.gaboom.transforms.evaluate;

import org.apache.beam.sdk.transforms.Combine;
import org.omegabyte.gaboom.Individual;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SortIndividualsFn<GenomeT extends Serializable> extends Combine.CombineFn<Individual<GenomeT>, List<Individual<GenomeT>>, List<Individual<GenomeT>>> {

    @Override
    public List<Individual<GenomeT>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Individual<GenomeT>> addInput(List<Individual<GenomeT>> individuals, Individual<GenomeT> individual) {
        for (int i = 0; i < individuals.size(); i++) {
            if (individual.getFitness() < individuals.get(i).getFitness()) {
                individuals.add(i, individual);
                return individuals;
            }
        }
        individuals.add(individual);
        return individuals;
    }

    @Override
    public List<Individual<GenomeT>> mergeAccumulators(Iterable<List<Individual<GenomeT>>> iterable) {
        List<Individual<GenomeT>> current = new ArrayList<>();
        for (List<Individual<GenomeT>> individuals : iterable) {
            final List<Individual<GenomeT>> next = new ArrayList<>();
            int currentIndex = 0;
            int individualsIndex = 0;

            while (currentIndex < current.size() && individualsIndex < individuals.size()) {
                if (current.get(currentIndex).getFitness() < individuals.get(individualsIndex).getFitness()) {
                    next.add(current.get(currentIndex++));
                } else {
                    next.add(individuals.get(individualsIndex++));
                }
            }
            if (currentIndex < current.size()) {
                next.addAll(current.subList(currentIndex, current.size()));
            }
            if (individualsIndex < individuals.size()) {
                next.addAll(individuals.subList(individualsIndex, individuals.size()));
            }
            current = next;
        }
        return current;
    }

    @Override
    public List<Individual<GenomeT>> extractOutput(List<Individual<GenomeT>> individuals) {
        return individuals;
    }
}