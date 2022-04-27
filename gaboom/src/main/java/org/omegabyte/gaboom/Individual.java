package org.omegabyte.gaboom;

import org.omegabyte.gaboom.utils.IdGenerator;

import java.io.Serializable;
import java.util.Random;

public class Individual<GenomeT extends Serializable> implements Serializable {
    private final String id;
    private final GenomeT genome;
    private Double fitness = null;

    public Individual(String id, GenomeT genome) {
        this.id = id;
        this.genome = genome;
    }

    public Individual(Random random, GenomeT genome) {
        this.id = IdGenerator.newId(random, 6);
        this.genome = genome;
    }

    public String getId() {
        return id;
    }

    public GenomeT getGenome() {
        return genome;
    }

    public Double getFitness() {
        return fitness;
    }

    public void setFitness(Double fitness) {
        this.fitness = fitness;
    }
}
