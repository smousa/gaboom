package org.omegabyte.gaboom.transforms.evaluate;

import org.apache.beam.sdk.transforms.Combine;
import org.omegabyte.gaboom.Individual;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortIndividualsFn<GenomeT extends Serializable> extends Combine.CombineFn<Individual<GenomeT>, List<Individual<GenomeT>>, List<Individual<GenomeT>>> {

    public static <GenomeT extends Serializable> int compare(Individual<GenomeT> i1, Individual<GenomeT> i2) {
        return Comparator.comparingDouble(Individual<GenomeT>::getFitness).thenComparing(Individual::getId).compare(i1, i2);
    }

    @Override
    public List<Individual<GenomeT>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Individual<GenomeT>> addInput(List<Individual<GenomeT>> accumulator, Individual<GenomeT> input) {

        for (int i = 0; i < accumulator.size(); i++) {
            Individual<GenomeT> individual = accumulator.get(i);
            if (compare(input, individual) < 0) {
                accumulator.add(i, input);
                return accumulator;
            }
        }
        accumulator.add(input);
        return accumulator;
    }

    @Override
    public List<Individual<GenomeT>> mergeAccumulators(Iterable<List<Individual<GenomeT>>> iterable) {
        List<Individual<GenomeT>> result = new ArrayList<>();
        for (List<Individual<GenomeT>> input : iterable) {
            List<Individual<GenomeT>> merged = new ArrayList<>();
            int resultIndex = 0;
            int inputIndex = 0;

            while (resultIndex < result.size() && inputIndex < input.size()) {
                Individual<GenomeT> resultIndividual = result.get(resultIndex);
                Individual<GenomeT> inputIndividual = input.get(inputIndex);
                if (compare(resultIndividual, inputIndividual) < 0) {
                    merged.add(result.get(resultIndex++));
                } else {
                    merged.add(input.get(inputIndex++));
                }
            }

            if (resultIndex < result.size()) {
                merged.addAll(result.subList(resultIndex, result.size()));
            } else if (inputIndex < input.size()) {
                merged.addAll(input.subList(inputIndex, input.size()));
            }
            result = merged;
        }
        return result;
    }

    @Override
    public List<Individual<GenomeT>> extractOutput(List<Individual<GenomeT>> individuals) {
        return individuals;
    }
}