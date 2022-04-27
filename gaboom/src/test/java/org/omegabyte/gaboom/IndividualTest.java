package org.omegabyte.gaboom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class IndividualTest {

    @Test
    @DisplayName("It should return the id and the genome")
    void testIndividual() {
        Individual<String> individual = new Individual<>("testIndividual", "test");
        assertEquals("testIndividual", individual.getId());
        assertEquals("test", individual.getGenome());
        assertNull(individual.getFitness());
    }

    @Test
    @DisplayName("It should return the random id")
    void testIndividualWithRandomID() {
        Random r = new Random();
        r.setSeed(0);
        Individual<String> individual = new Individual<>(r, "test");
        assertEquals("SSXVNJ", individual.getId());
        assertEquals("test", individual.getGenome());
        assertNull(individual.getFitness());
    }

    @Test
    @DisplayName("It should return the fitness if it is set")
    void testSetFitness() {
        Individual<String> individual = new Individual<>("testIndividual", "test");
        individual.setFitness(12.4);
        assertEquals(12.4, individual.getFitness().doubleValue());
    }
}