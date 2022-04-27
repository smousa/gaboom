package org.omegabyte.gaboom.transforms.select;

import org.apache.beam.sdk.values.KV;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;
import org.omegabyte.gaboom.SelectIndividuals;
import org.omegabyte.gaboom.transforms.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * SelectEliteFn returns the best n individuals from the population.
 * @param <GenomeT>
 */
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
        List<Individual<GenomeT>> individuals = new ArrayList<>();
        for (int i = 0; i < selectIndividuals.getN(); i++) {
            indices.add(i);
            individuals.add(selectIndividuals.getIndividuals().get(i));
        }
        c.output(selectIndicesTupleTag, KV.of(key, indices));
        c.output(KV.of(key, new Individuals<>(selectIndividuals.getSeed(), individuals)));
    }
}
