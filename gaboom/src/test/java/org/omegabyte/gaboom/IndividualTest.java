package org.omegabyte.gaboom;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IndividualTest {

    @Test
    //@DisplayName("It should return the id and the genome")
    public void testIndividual() {
        Individual<String> individual = new Individual<>("testIndividual", "test");
        assertEquals("testIndividual", individual.getId());
        assertEquals("test", individual.getGenome());
        assertNull(individual.getFitness());
    }

    @Test
    //@DisplayName("It should return the random id")
    public void testIndividualWithRandomID() {
        Random r = new Random();
        r.setSeed(0);
        Individual<String> individual = new Individual<>(r, "test");
        assertEquals("22Pbd7", individual.getId());
        assertEquals("test", individual.getGenome());
        assertNull(individual.getFitness());
    }

    @Test
    //@DisplayName("It should return the fitness if it is set")
    public void testSetFitness() {
        Individual<String> individual = new Individual<>("testIndividual", "test");
        individual.setFitness(12.4);
        assertEquals(12.4, individual.getFitness().doubleValue(), 0);
    }
}