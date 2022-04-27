package org.omegabyte.gaboom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BaseItemTest {

    @Test
    @DisplayName("It should return the seed")
    public void testGetSeed() {
        long seed = new Date().getTime();
        BaseItem baseItem = new BaseItem(seed);
        assertEquals(seed, baseItem.getSeed());
    }

    @Test
    @DisplayName("It should update the seed")
    public void testSetSeed() {
        BaseItem baseItem = new BaseItem(0);
        assertEquals(0, baseItem.getSeed());
        baseItem.setSeed(500);
        assertEquals(500, baseItem.getSeed());
    }

    @Test
    @DisplayName("It should return a random generator")
    public void testGetRandomGenerator() {
        BaseItem baseItem = new BaseItem(0);
        Random expected = new Random();
        expected.setSeed(0);
        assertEquals(expected.nextLong(), baseItem.getRandomGenerator().nextLong());
    }
}