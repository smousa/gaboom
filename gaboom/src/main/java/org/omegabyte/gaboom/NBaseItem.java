package org.omegabyte.gaboom;

import java.io.Serializable;

public class NBaseItem extends BaseItem implements Serializable {
    private final int n;

    public NBaseItem(long seed, int n) {
        super(seed);
        this.n = n;
    }

    public int getN() {
        return n;
    }
}