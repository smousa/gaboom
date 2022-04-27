package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.text.RandomStringGenerator;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.transforms.Crossover;
import org.omegabyte.gaboom.transforms.Evaluate;
import org.omegabyte.gaboom.transforms.Mutate;
import org.omegabyte.gaboom.transforms.Populate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ModelTest {
    private static final RandomStringGenerator.Builder builder = new RandomStringGenerator.Builder()
            .withinRange('a', 'z');

    public static String SECRET_WORD;

    public static class PopulateFn extends Populate.PopulateFn<String> implements Serializable {
        @Override
        public String makeGenome(ProcessContext context, Random random) {
            return builder.usingRandom(random::nextInt).build().generate(SECRET_WORD.length());
        }
    }

    public static class CrossoverFn extends Crossover.CrossoverFn<String> implements Serializable {
        @Override
        public String crossover(ProcessContext context, Random random, String p1, String p2) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < SECRET_WORD.length(); i++) {
                if (random.nextBoolean()) {
                    result.append(p1.charAt(i));
                } else {
                    result.append(p2.charAt(i));
                }
            }
            return result.toString();
        }
    }

    public static class MutateFn extends Mutate.MutateFn<String> implements Serializable {
        private final int mutations;

        public MutateFn(int mutations) {
            this.mutations = mutations;
        }

        @Override
        public String mutate(ProcessContext context, Random random, String genome) {
            List<Integer> indices = new ArrayList<>();
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < genome.length(); i++) {
                indices.add(i);
                result.append(genome.charAt(i));
            }

            for (int i = 0; i < mutations; i++) {
                int index = indices.remove(random.nextInt(indices.size()));
                char c = (char) (random.nextInt(26) + 'a');
                result.setCharAt(index, c);
            }

            return result.toString();
        }
    }

    public static class FitnessTransform extends Evaluate.FitnessTransform<String> implements Serializable {
        static class FitnessFn extends DoFn<Individual<String>, Individual<String>> {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Individual<String> individual = new Individual<>(c.element().getId(), c.element().getGenome());
                double score = 0;
                for (int i = 0; i < SECRET_WORD.length(); i++) {
                    score += Math.abs(individual.getGenome().charAt(i) - SECRET_WORD.charAt(i));
                }
                individual.setFitness(score);
                c.output(individual);
            }
        }

        @Override
        public PCollection<Individual<String>> expand(PCollection<Individual<String>> individualPCollection) {
            return individualPCollection.apply(ParDo.of(new FitnessFn()));
        }
    }
}
