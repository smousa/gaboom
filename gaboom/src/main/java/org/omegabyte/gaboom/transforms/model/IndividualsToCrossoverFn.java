package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.CrossoverIndividuals;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class IndividualsToCrossoverFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, CrossoverIndividuals<GenomeT>>> {
    private static final Logger logger = LoggerFactory.getLogger(IndividualsToCrossoverFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Individuals<GenomeT> individuals = c.element().getValue();

        if (individuals.getIndividuals().size() != 2) {
            logger.error("Selection size {} is invalid for crossover, id={}", individuals.getIndividuals().size(), key);
            return;
        }

        Individual<GenomeT> p1 = individuals.getIndividuals().get(0);
        Individual<GenomeT> p2 = individuals.getIndividuals().get(1);
        c.output(KV.of(key, new CrossoverIndividuals<>(individuals.getSeed(), p1, p2)));
    }
}