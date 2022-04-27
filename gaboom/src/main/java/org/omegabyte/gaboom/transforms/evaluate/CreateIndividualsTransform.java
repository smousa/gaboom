package org.omegabyte.gaboom.transforms.evaluate;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;
import java.util.List;

public class CreateIndividualsTransform<GenomeT extends Serializable> extends PTransform<PCollection<KV<String, List<Individual<GenomeT>>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
    private final PCollection<KV<String, BaseItem>> baseItem;

    public CreateIndividualsTransform(PCollection<KV<String, BaseItem>> baseItem) {
        this.baseItem = baseItem;
    }

    public static <GenomeT extends Serializable> CreateIndividualsTransform<GenomeT> of(PCollection<KV<String, BaseItem>> baseItem) {
        return new CreateIndividualsTransform<>(baseItem);
    }

    static class CreateIndividualsFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individuals<GenomeT>>> {
        private final TupleTag<BaseItem> baseItemTT;
        private final TupleTag<List<Individual<GenomeT>>> individualsTT;

        public CreateIndividualsFn(TupleTag<BaseItem> baseItemTT, TupleTag<List<Individual<GenomeT>>> individualsTT) {
            this.baseItemTT = baseItemTT;
            this.individualsTT = individualsTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            CoGbkResult result = c.element().getValue();
            BaseItem baseItem = result.getOnly(baseItemTT);
            List<Individual<GenomeT>> individuals = result.getOnly(individualsTT);
            c.output(KV.of(key, new Individuals<>(baseItem.getSeed(), individuals)));
        }
    }

    @Override
    public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, List<Individual<GenomeT>>>> input) {
        TupleTag<BaseItem> baseItemTT = new TupleTag<>();
        TupleTag<List<Individual<GenomeT>>> individualsTT = new TupleTag<>();
        return KeyedPCollectionTuple.of(baseItemTT, baseItem).and(individualsTT, input)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new CreateIndividualsFn<>(baseItemTT, individualsTT)));
    }
}
