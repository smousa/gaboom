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
import org.omegabyte.gaboom.Individual;

import java.io.Serializable;

public class AssignIndividualTransform<GenomeT extends Serializable> extends PTransform<PCollection<KV<String, Individual<GenomeT>>>, PCollection<KV<String, Individual<GenomeT>>>> {
    private final PCollection<KV<String, String>> keyAtId;

    public AssignIndividualTransform(PCollection<KV<String, String>> keyAtId) {
        this.keyAtId = keyAtId;
    }

    public static <GenomeT extends Serializable> AssignIndividualTransform<GenomeT> of(PCollection<KV<String, String>> keyAtId) {
        return new AssignIndividualTransform<>(keyAtId);
    }

    static class AssignIndividualFn<GenomeT extends Serializable> extends DoFn<KV<String, CoGbkResult>, KV<String, Individual<GenomeT>>> {
        private final TupleTag<String> indexTT;
        private final TupleTag<Individual<GenomeT>> individualTT;

        public AssignIndividualFn(TupleTag<String> indexTT, TupleTag<Individual<GenomeT>> individualTT) {
            this.indexTT = indexTT;
            this.individualTT = individualTT;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            CoGbkResult result = c.element().getValue();
            String key = result.getOnly(indexTT);
            Individual<GenomeT> individual = result.getOnly(individualTT);
            c.output(KV.of(key, individual));
        }
    }

    @Override
    public PCollection<KV<String, Individual<GenomeT>>> expand(PCollection<KV<String, Individual<GenomeT>>> input) {
        TupleTag<String> indexTT = new TupleTag<>();
        TupleTag<Individual<GenomeT>> individualTT = new TupleTag<>();
        return KeyedPCollectionTuple.of(individualTT, input).and(indexTT, keyAtId)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new AssignIndividualFn<>(indexTT, individualTT)));
    }
}