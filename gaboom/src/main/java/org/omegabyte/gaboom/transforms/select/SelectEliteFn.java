package org.omegabyte.gaboom.transforms.select;

import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SelectEliteFn<GenomeT> extends Select.SelectFn<GenomeT> {
    private static final Logger logger = LoggerFactory.getLogger(SelectEliteFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        SelectIndividuals<GenomeT> selectIndividuals = c.element().getValue();

        if (selectIndividuals.getN() > selectIndividuals.getIndividuals().size()) {
            logger.error("Select size {} is greater than individual list {}, key={}", selectIndividuals.getN(), selectIndividuals.getIndividuals().size(), key);
            return;
        }

        List<Integer> indices = new ArrayList<>();
        Individuals<GenomeT> individuals = new Individuals<>(selectIndividuals.getSeed());
        for (int i = 0; i < selectIndividuals.getN(); i++) {
            indices.add(i);
            individuals.getIndividuals().add(selectIndividuals.getIndividuals().get(i));
        }
        c.output(selectIndicesTupleTag, KV.of(key, indices));
        c.output(KV.of(key, individuals));
    }
}
