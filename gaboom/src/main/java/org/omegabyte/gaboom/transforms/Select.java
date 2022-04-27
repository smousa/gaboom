package org.omegabyte.gaboom.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.*;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;

import java.util.List;

public class Select {

    public static abstract class SelectFn<GenomeT> extends DoFn<KV<String, SelectIndividuals<GenomeT>>, KV<String, Individuals<GenomeT>>> {
        protected TupleTag<KV<String, List<Integer>>> selectIndicesTupleTag;

        public SelectFn(TupleTag<KV<String, List<Integer>>> selectIndicesTupleTag) {
            this.selectIndicesTupleTag = selectIndicesTupleTag;
        }

        public SelectFn() {
            this.selectIndicesTupleTag =  new TupleTag<>();
        }
    }

    public static class SelectNoIndexTransform<GenomeT> extends PTransform<PCollection<KV<String, SelectIndividuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {
        private final SelectFn<GenomeT> fn;

        public SelectNoIndexTransform(SelectFn<GenomeT> fn) {
            this.fn = SerializableUtils.clone(fn);
        }

        @Override
        public PCollection<KV<String, Individuals<GenomeT>>> expand(PCollection<KV<String, SelectIndividuals<GenomeT>>> input) {
            return input.apply(ParDo.of(fn));
        }
    }

    public static class SelectIndexTransform<GenomeT> extends PTransform<PCollection<KV<String, SelectIndividuals<GenomeT>>>, PCollectionTuple> {
        private final TupleTag<KV<String, List<Integer>>> selectIndicesTupleTag;
        private final TupleTag<KV<String, Individuals<GenomeT>>> selectIndividualsTupleTag;
        private final SelectFn<GenomeT> fn;

        public SelectIndexTransform(TupleTag<KV<String, List<Integer>>> selectIndicesTupleTag, TupleTag<KV<String, Individuals<GenomeT>>> selectIndividualsTupleTag, SelectFn<GenomeT> fn) {
            this.selectIndicesTupleTag = selectIndicesTupleTag;
            this.selectIndividualsTupleTag = selectIndividualsTupleTag;
            this.fn = SerializableUtils.clone(fn);
            this.fn.selectIndicesTupleTag = selectIndicesTupleTag;
        }

        @Override
        public PCollectionTuple expand(PCollection<KV<String, SelectIndividuals<GenomeT>>> input) {
            return input.apply(ParDo.of(fn).withOutputTags(selectIndividualsTupleTag, TupleTagList.of(selectIndicesTupleTag)));
        }
    }

    public static <GenomeT> SelectNoIndexTransform<GenomeT> as(SelectFn<GenomeT> selectFn) {
        return new SelectNoIndexTransform<>(selectFn);
    }

    public static <GenomeT> SelectIndexTransform<GenomeT> as(SelectFn<GenomeT> selectFn, TupleTag<KV<String, List<Integer>>> idx, TupleTag<KV<String, Individuals<GenomeT>>> ind) {
        return new SelectIndexTransform<>(idx, ind, selectFn);
    }
}