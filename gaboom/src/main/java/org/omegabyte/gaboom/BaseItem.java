package org.omegabyte.gaboom;

import java.util.Random;

public class BaseItem {
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
