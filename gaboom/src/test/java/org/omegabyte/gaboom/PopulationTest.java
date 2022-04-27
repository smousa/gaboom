package org.omegabyte.gaboom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PopulationTest {

    @Test
    @DisplayName("It should return an empty population")
    void testPopulation() throws Exception {
        Population<Integer> population = new Population<>("test");
        assertEquals(0, population.getSeed());
        assertEquals("test", population.getId());
        assert population.getIndividuals().isEmpty();
        assert population.getGenerations() == -1;
        Thread.sleep(1);
        assert population.getAge() > 0;
    }

    @Test
    @DisplayName("It should update the population with the new individuals")
    void update() {
        List<Individual<Integer>> individualList = new ArrayList<>();
        individualList.add(new Individual<>("p1", 1));
        individualList.add(new Individual<>("p1", 2));
        Individuals<Integer> individuals = new Individuals<>(10, individualList);

        Population<Integer> population = new Population<>("test");
        population.update(individuals);
        assert population.getSeed() == 10;
        assert population.getIndividuals() == individuals.getIndividuals();
        assert population.getGenerations() == 0;

        individualList = new ArrayList<>();
        individualList.add(new Individual<>("p3", 3));
        individualList.add(new Individual<>("p4", 4));
        individuals = new Individuals<>(20, individualList);

        population.update(individuals);
        assert population.getSeed() == 20;
        assert population.getIndividuals() == individuals.getIndividuals();
        assert population.getGenerations() == 1;
    }
}