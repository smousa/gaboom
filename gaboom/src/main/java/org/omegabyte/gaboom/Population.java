package org.omegabyte.gaboom;

import org.apache.beam.sdk.util.SerializableUtils;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class Population<GenomeT extends Serializable> extends Individuals<GenomeT> implements Serializable {
    private final String id;
    private final long created = new Date().getTime();
    private int generations = -1;

    public Population(String id) {
        super(0);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public List<Individual<GenomeT>> getIndividuals() {
        return individuals;
    }

    public long getAge() {
        return new Date().getTime() - created;
    }

    public int getGenerations() {
        return generations;
    }

    public void update(Individuals<GenomeT> individuals) {
        setSeed(individuals.getSeed());
        this.individuals = individuals.getIndividuals();
        this.generations++;
    }

    public boolean isNew() {
        return generations < 0;
    }

    public Population<GenomeT> clone() {
        return SerializableUtils.clone(this);
    }
}