package org.omegabyte.gaboom;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CrossoverIndividualsTest {

    @Test
    //@DisplayName("It should get the individuals")
    public void testGetParents() {
        Individual<Integer> p1 = new Individual<>("p1", 1);
        Individual<Integer> p2 = new Individual<>("p2", 2);

        CrossoverIndividuals<Integer> crossoverIndividuals = new CrossoverIndividuals<>(0, p1, p2);
        assertEquals(p1, crossoverIndividuals.getP1());
        assertEquals(p2, crossoverIndividuals.getP2());
    }
}