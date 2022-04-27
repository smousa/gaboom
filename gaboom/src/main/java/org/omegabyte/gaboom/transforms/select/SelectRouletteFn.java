package org.omegabyte.gaboom.transforms.select;

import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Select;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SelectRouletteFn<GenomeT> extends Select.SelectFn<GenomeT> {

    private List<Double> getWheel(List<Individual<GenomeT>> individuals) {
        int n = individuals.size();
        double total = 0;

        List<Double> wheel = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            wheel.add(individuals.get(n-1).getFitness()-individuals.get(i).getFitness()+1);
            total += wheel.get(i);
        }
        return cumsum(divide(wheel, total));
    }

    private List<Double> divide(List<Double> doubles, Double value) {
        List<Double> divided = new ArrayList<>();
        doubles.forEach(d -> divided.add(d/value));
        return divided;
    }

    private List<Double> cumsum(List<Double> doubles) {
        List<Double> summed = new ArrayList<>();
        summed.add(doubles.get(0));
        for (int i = 1; i < doubles.size(); i++) {
            summed.add(doubles.get(i) - doubles.get(i-1));
        }
        return summed;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        SelectIndividuals<GenomeT> selectIndividuals = c.element().getValue();
        Random rng = selectIndividuals.getRandomGenerator();

        List<Integer> indices = new ArrayList<>();
        Individuals<GenomeT> individuals = new Individuals<>(rng.nextLong());
        List<Double> wheel = getWheel(selectIndividuals.getIndividuals());
        for (int i = 0; i < selectIndividuals.getN(); i++) {
            double value = rng.nextDouble();
            for (int j = 1; j < wheel.size(); j++) {
                if (value >= wheel.get(j)) {
                    indices.add(j);
                    individuals.getIndividuals().add(selectIndividuals.getIndividuals().get(j));
                    break;
                }
            }
            if (individuals.getIndividuals().size() < i) {
                indices.add(0);
                individuals.getIndividuals().add(selectIndividuals.getIndividuals().get(0));
            }
        }
        c.output(selectIndicesTupleTag, KV.of(key, indices));
        c.output(KV.of(key, individuals));
    }
}
