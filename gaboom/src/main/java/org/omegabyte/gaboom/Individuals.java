package org.omegabyte.gaboom;

import java.util.Collections;
import java.util.List;

public class Individuals<GenomeT> extends BaseItem {
    private final List<Individual<GenomeT>> individuals;

    public Individuals(long seed) {
        super(seed);
        this.individuals = Collections.emptyList();
    }

    public Individuals(long seed, List<Individual<GenomeT>> individuals) {
        super(seed);
        this.individuals = individuals;
    }

    public List<Individual<GenomeT>> getIndividuals() {
        return individuals;
    }
}