package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.Random;

import static org.omegabyte.gaboom.Individual.ID_LENGTH;

public class SetMutantNameFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, KV<String, Individuals<GenomeT>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Random rng = c.element().getValue().getRandomGenerator();

        Individuals<GenomeT> result = new Individuals<>(rng.nextLong());
        c.element().getValue().getIndividuals().forEach(i -> {
            if (i.getId().length() > ID_LENGTH) {
                Individual<GenomeT> individual = new Individual<>(rng, i.getGenome());
                individual.setFitness(i.getFitness());
                result.getIndividuals().add(individual);
            } else {
                result.getIndividuals().add(i);
            }
        });
        c.output(KV.of(key, result));
    }
}
