package org.omegabyte.gaboom;

import java.io.Serializable;
import java.util.Random;

public class BaseItem implements Serializable {
    private long seed;

    public BaseItem(long seed) {
        this.seed = seed;
    }

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    public Random getRandomGenerator() {
        Random r = new Random();
        r.setSeed(seed);
        return r;
    }
}
