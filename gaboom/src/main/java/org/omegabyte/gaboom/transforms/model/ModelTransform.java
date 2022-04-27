package org.omegabyte.gaboom.transforms.model;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.omegabyte.gaboom.Individuals;

import java.io.Serializable;

public abstract class ModelTransform<GenomeT extends Serializable> extends PTransform<PCollection<KV<String, Individuals<GenomeT>>>, PCollection<KV<String, Individuals<GenomeT>>>> {}
