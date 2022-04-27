package org.omegabyte.gaboom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NBaseItemTest {

    @Test
    @DisplayName("It should return n")
    public void testGetN() {
        NBaseItem nBaseItem = new NBaseItem(99, 10);
        assertEquals(10, nBaseItem.getN());
    }
}