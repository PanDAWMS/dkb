/**
 * 2017, Golosova M (KIAE)
 *
 */

package ru.kiae.dkb.kafka.streams.topology;


import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import java.lang.Class;


public class ConfiguredTopologyBuilder extends TopologyBuilder {

  private static final Logger log = LoggerFactory.getLogger(ConfiguredTopologyBuilder.class);

  private final TopologyConfig config;

  public ConfiguredTopologyBuilder(TopologyConfig config) {
    this.config = config;
    configureTopology();
  }

  /* Creates topology nodes and links them together
   * accrding to the config.
   */
  private void configureTopology() {
    config.sourceConfigs.forEach(c -> addSource(c));
    config.processorConfigs.forEach(c -> addProcessor(c));
    config.storeConfigs.forEach(c -> addStore(c));
    config.sinkConfigs.forEach(c -> addSink(c));
  }

  private void addSource(SourceNodeConfig c) {
    try {
      super.addSource(c.name, (String[]) c.topics.toArray());
    } catch (Exception e) {
      log.error("Failed to add source node \"{}\"", c.name, e);
    }
  }

  private void addProcessor(ProcessorNodeConfig c) {
    try {
      ProcessorSupplier processorSupplier;
      Map<String, Object> props = c.processorProps();
      if (props.size() == 0)
        processorSupplier = c.getInitializedInstance(ProcessorNodeConfig.PROCESSOR_SUPPLIER_CLASS, ProcessorSupplier.class);
      else
        processorSupplier = c.getInitializedInstance(ProcessorNodeConfig.PROCESSOR_SUPPLIER_CLASS, ProcessorSupplier.class, props);
      super.addProcessor(c.name, processorSupplier, (String[]) c.parents.toArray());
    } catch (Exception e) {
      log.error("Failed to add processor node \"{}\"", c.name, e);
    }
  }

  private void addStore(StateStoreConfig c) {
    try {
      StateStoreSupplier kvStore = Stores.create(c.name).withKeys(c.keySerde).withValues(c.valSerde).inMemory().build();
      super.addStateStore(kvStore, (String[]) c.processors.toArray());
    } catch (Exception e) {
      log.error("Failed to add State Store \"{}\"", c.name, e);
    }
  }

  private void addSink(SinkNodeConfig c) {
    try {
      super.addSink(c.name, c.topic, (String[]) c.parents.toArray());
    } catch (Exception e) {
      log.error("Failed to add sink node \"{}\"", c.name, e);
    }
  }
}
