package org.omegabyte.gaboom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Individuals<GenomeT extends Serializable> extends BaseItem  implements Serializable {
    protected List<Individual<GenomeT>> individuals;

    public Individuals(long seed) {
        super(seed);
        this.individuals = new ArrayList<>();
    }

    public Individuals(long seed, List<Individual<GenomeT>> individuals) {
        super(seed);
        this.individuals = individuals;
    }

    public List<Individual<GenomeT>> getIndividuals() {
        return individuals;
    }
}