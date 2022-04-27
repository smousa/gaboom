package org.omegabyte.gaboom.transforms.evaluate;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.omegabyte.gaboom.BaseItem;
import org.omegabyte.gaboom.Individual;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;

public class ExpandIndividualsFn<GenomeT extends Serializable> extends DoFn<KV<String, Individuals<GenomeT>>, Individual<GenomeT>> {
    private final TupleTag<KV<String, String>> keyAtIdTT;
    private final TupleTag<KV<String, BaseItem>> baseItemAtKeyTT;
    private final TupleTag<KV<String, Individual<GenomeT>>> evaluatedAtKeyTT;

    public ExpandIndividualsFn(TupleTag<KV<String, String>> keyAtIdTT, TupleTag<KV<String, BaseItem>> baseItemAtKeyTT, TupleTag<KV<String, Individual<GenomeT>>> evaluatedAtKeyTT) {
        this.keyAtIdTT = keyAtIdTT;
        this.baseItemAtKeyTT = baseItemAtKeyTT;
        this.evaluatedAtKeyTT = evaluatedAtKeyTT;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Individuals<GenomeT> individuals = c.element().getValue();

        individuals.getIndividuals().forEach(ind -> {
            if (ind.getFitness() == null) {
                c.output(keyAtIdTT, KV.of(ind.getId(), key));
                c.output(ind);
            } else {
                c.output(evaluatedAtKeyTT, KV.of(key, ind));
            }
        });
        c.output(baseItemAtKeyTT, KV.of(key, individuals));
    }
}
