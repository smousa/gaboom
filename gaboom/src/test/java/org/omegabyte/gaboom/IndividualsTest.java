package org.omegabyte.gaboom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndividualsTest {

    @Test
    @DisplayName("It should get individuals")
    void testGetIndividuals() {
        List<Individual<Integer>> individualList = new ArrayList<>();
        individualList.add(new Individual<>("p1", 1));
        individualList.add(new Individual<>("p1", 2));

        Individuals<Integer> individuals = new Individuals<>(0, individualList);
        assertEquals(individualList, individuals.getIndividuals());
    }

    @Test
    @DisplayName("It should return an empty list of individuals")
    void testGetEmptyIndividuals() {
        Individuals<Integer> individuals = new Individuals<>(0);
        assertEquals(0, individuals.getIndividuals().size());
    }
}