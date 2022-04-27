package org.omegabyte.gaboom;

public class NBaseItem extends BaseItem {
    private final int n;

    public NBaseItem(long seed, int n) {
        super(seed);
        this.n = n;
    }

    public int getN() {
        return n;
    }
}