package org.omegabyte.gaboom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SelectIndividualsTest {

    @Test
    @DisplayName("It should return n")
    public void testGetN() {
        SelectIndividuals<Integer> selectIndividuals = new SelectIndividuals<>(0, Collections.emptyList(), 4);
        assertEquals(4, selectIndividuals.getN());
    }

    @Test
    @DisplayName("It should set values base on the individuals")
    public void testSelectIndividualsFromIndividuals() {
        List<Individual<Integer>> individualList = new ArrayList<>();
        individualList.add(new Individual<>("p1", 1));
        individualList.add(new Individual<>("p1", 2));
        Individuals<Integer> individuals = new Individuals<>(10, individualList);

        SelectIndividuals<Integer> selectIndividuals = new SelectIndividuals<>(individuals, 4);
        assertEquals(individuals.getSeed(), selectIndividuals.getSeed());
        assertEquals(individuals.getIndividuals(), selectIndividuals.getIndividuals());
        assertEquals(4, selectIndividuals.getN());
    }

}