package org.omegabyte.gaboom.utils;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class IdGeneratorTest {

    @Test
    // It should generate an id
    public void testNewId() {
        Random r = new Random();
        r.setSeed(0);
        assertEquals("22Pbd7", IdGenerator.newId(r, 6));
    }

}