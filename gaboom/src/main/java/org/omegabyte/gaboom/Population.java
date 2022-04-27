package org.omegabyte.gaboom;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Population<GenomeT> extends BaseItem {
    private final String id;
    private final long created = new Date().getTime();
    private List<Individual<GenomeT>> individuals;
    private int generations = -1;

    public Population(long seed, String id) {
        super(seed);
        this.id = id;
        this.individuals = new ArrayList<>();
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
}