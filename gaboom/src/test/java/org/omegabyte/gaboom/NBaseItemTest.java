package org.omegabyte.gaboom;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NBaseItemTest {

    @Test
    //@DisplayName("It should return n")
    public void testGetN() {
        NBaseItem nBaseItem = new NBaseItem(99, 10);
        assertEquals(10, nBaseItem.getN());
    }
}