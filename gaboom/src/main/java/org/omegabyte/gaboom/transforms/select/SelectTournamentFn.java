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
import java.util.Random;

/**
 * SelectTournamentFn samples individuas through tournament selection.  The
 * tournament is composed of randomly chosen individuals.  The winner of the
 * tournament is the chosen individual with the lowest fitness.  The obtained
 * individuals are all distinct.
 * @param <GenomeT>
 */
public class SelectTournamentFn<GenomeT> extends Select.SelectFn<GenomeT> {
    private static final Logger logger = LoggerFactory.getLogger(SelectTournamentFn.class);

    private final int nContestants;

    public SelectTournamentFn(int nContestants) {
        this.nContestants = nContestants;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        SelectIndividuals<GenomeT> selectIndividuals = c.element().getValue();
        Random rng = selectIndividuals.getRandomGenerator();

        if (selectIndividuals.getIndividuals().size() - nContestants < selectIndividuals.getN()) {
            logger.error("Not enough individuals for tournament selection, id={}", key);
            return;
        }

        List<Integer> options = new ArrayList<>();
        for (int i = 0; i < selectIndividuals.getIndividuals().size(); i++) {
            options.add(i);
        }

        List<Integer> indices = new ArrayList<>();
        List<Individual<GenomeT>> individualList = new ArrayList<>();
        for (int i = 0; i < selectIndividuals.getN(); i++) {
            int maxIndex = options.size();
            for (int j = 0; j < nContestants; j++) {
                int nextIndex = rng.nextInt(options.size());
                if (nextIndex < maxIndex) {
                    maxIndex = nextIndex;
                }
            }

            int value = options.remove(maxIndex);
            indices.add(value);
            individualList.add(selectIndividuals.getIndividuals().get(value));
            options.remove(maxIndex);
        }
        c.output(selectIndicesTupleTag, KV.of(key, indices));
        c.output(KV.of(key, new Individuals<>(rng.nextLong(), individualList)));
    }
}
