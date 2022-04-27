package org.omegabyte.gaboom.transforms.select;

import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SelectRouletteFn<GenomeT> extends Select.SelectFn<GenomeT> {
    private static final Logger logger = LoggerFactory.getLogger(SelectRouletteFn.class);

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
            summed.add(doubles.get(i) + summed.get(i-1));
        }
        return summed;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        SelectIndividuals<GenomeT> selectIndividuals = c.element().getValue();

        if (selectIndividuals.getIndividuals().size() == 0) {
            logger.error("Select individuals is 0, key={}", key);
            return;
        }

        Random rng = selectIndividuals.getRandomGenerator();
        List<Integer> indices = new ArrayList<>();
        List<Individual<GenomeT>> individuals = new ArrayList<>();

        List<Double> wheel = getWheel(selectIndividuals.getIndividuals());
        for (int i = 0; i < selectIndividuals.getN(); i++) {
            double value = rng.nextDouble();
            for (int j = 0; j < wheel.size(); j++) {
                if (value < wheel.get(j)) {
                    indices.add(j);
                    individuals.add(selectIndividuals.getIndividuals().get(j));
                    break;
                }
            }
        }
        c.output(selectIndicesTupleTag, KV.of(key, indices));
        c.output(KV.of(key, new Individuals<>(rng.nextLong(), individuals)));
    }
}