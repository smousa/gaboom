package org.omegabyte.gaboom;

import java.io.Serializable;

public class CrossoverIndividuals<GenomeT extends Serializable> extends BaseItem implements Serializable {
    private final Individual<GenomeT> p1;
    private final Individual<GenomeT> p2;

    public CrossoverIndividuals(long seed, Individual<GenomeT> p1, Individual<GenomeT> p2) {
        super(seed);
        this.p1 = p1;
        this.p2 = p2;
    }

    public Individual<GenomeT> getP1() { return p1; }

    public Individual<GenomeT> getP2() { return p2; }
}
