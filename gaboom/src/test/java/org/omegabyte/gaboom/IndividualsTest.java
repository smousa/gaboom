package org.omegabyte.gaboom;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IndividualsTest {

    @Test
    //@DisplayName("It should get individuals")
    public void testGetIndividuals() {
        List<Individual<Integer>> individualList = new ArrayList<>();
        individualList.add(new Individual<>("p1", 1));
        individualList.add(new Individual<>("p1", 2));

        Individuals<Integer> individuals = new Individuals<>(0, individualList);
        assertEquals(individualList, individuals.getIndividuals());
    }

    @Test
    //@DisplayName("It should return an empty list of individuals")
    public void testGetEmptyIndividuals() {
        Individuals<Integer> individuals = new Individuals<>(0);
        assertEquals(0, individuals.getIndividuals().size());
    }
}