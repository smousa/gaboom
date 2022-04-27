package org.omegabyte.gaboom.utils;

import org.apache.commons.text.RandomStringGenerator;

import java.util.Random;

public class IdGenerator {
    public static String newId(Random r, int length) {
        RandomStringGenerator rsg = new RandomStringGenerator.Builder()
                .withinRange(new char[]{'0', '9'}, new char[]{'a', 'z'}, new char[]{'A', 'Z'})
                .usingRandom(r::nextInt)
                .build();
        return rsg.generate(length);
    }
}