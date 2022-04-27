package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.utils.IdGenerator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PopulateTest {

    static class SimplePopulateFn extends Populate.PopulateFn<Integer> implements Serializable {
        @Override
        public Integer makeGenome(ProcessContext context, Random random) {
            return random.nextInt();
        }
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    // It should build a population of a given size
    public void testPopulateTransform() {
        Populate.PopulateFn<Integer> populateFn = new SimplePopulateFn();

        PCollectionView<Integer> sideInput = pipeline.apply("SideInput", Create.of(4))
                .apply(View.asSingleton());

        PCollection<Individuals<Integer>> population =  pipeline
                .apply(Create.of(new BaseItem(0)))
                .apply(Populate.as(populateFn).withPopSize(3).withSideInputs(sideInput));

        PAssert.that(population).satisfies((SerializableFunction<Iterable<Individuals<Integer>>, Void>) individuals -> {
            Random random = new Random();
            random.setSeed(0);

            List<Individuals<Integer>> individualsList = new ArrayList<>();
            individuals.forEach(individualsList::add);
            Assert.assertEquals(1, individualsList.size());
            individualsList.forEach(indis-> {
                Assert.assertEquals(random.nextLong(), indis.getSeed());
                Assert.assertEquals(3, indis.getIndividuals().size());

                indis.getIndividuals().forEach(ind -> {
                    Assert.assertEquals(random.nextInt(), (int) ind.getGenome());
                    Assert.assertEquals(IdGenerator.newId(random, 6), ind.getId());
                });
            });
            return null;
        });

        pipeline.run();
    }

}