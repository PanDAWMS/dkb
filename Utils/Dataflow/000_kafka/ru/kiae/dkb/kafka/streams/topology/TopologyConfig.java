/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.topology;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.lang.reflect.Constructor;
import java.lang.Class;


public class TopologyConfig extends AbstractConfig {

  private static final Logger log = LoggerFactory.getLogger(TopologyConfig.class);

  public final List<SourceNodeConfig> sourceConfigs;
  public final List<ProcessorNodeConfig> processorConfigs;
  public final List<StateStoreConfig> storeConfigs;
  public final List<SinkNodeConfig> sinkConfigs;
  public final List<TopologyNodeConfig> nodeConfigs;

  public static final String SOURCES_IDS_CONFIG = "sources.ids";
  private static final String SOURCES_IDS_DOC = "List of the source node names.";
  private static final String SOURCES_IDS_DISPLAY = "Sources";

  public static final String PROCESSORS_IDS_CONFIG = "processors.ids";
  private static final String PROCESSORS_IDS_DOC = "List of the processor node names.";
  private static final String PROCESSORS_IDS_DISPLAY = "Processors";

  public static final String STORES_IDS_CONFIG = "stores.ids";
  private static final String STORES_IDS_DEFAULT = "";
  private static final String STORES_IDS_DOC = "List of the state store names.";
  private static final String STORES_IDS_DISPLAY = "Stores";

  public static final String SINKS_IDS_CONFIG = "sinks.ids";
  private static final String SINKS_IDS_DOC = "List of the sink node names.";
  private static final String SINKS_IDS_DISPLAY = "Sinks";

  public static final String TOPOLOGY_GROUP = "topology";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
      // Topology
      .define(SOURCES_IDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, SOURCES_IDS_DOC,
              TOPOLOGY_GROUP, 1, ConfigDef.Width.LONG, SOURCES_IDS_DISPLAY)
      .define(PROCESSORS_IDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, PROCESSORS_IDS_DOC,
              TOPOLOGY_GROUP, 2, ConfigDef.Width.LONG, PROCESSORS_IDS_DISPLAY)
      .define(STORES_IDS_CONFIG, ConfigDef.Type.LIST, STORES_IDS_DEFAULT,
              ConfigDef.Importance.HIGH, STORES_IDS_DOC,
              TOPOLOGY_GROUP, 3, ConfigDef.Width.LONG, STORES_IDS_DISPLAY)
      .define(SINKS_IDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, SINKS_IDS_DOC,
              TOPOLOGY_GROUP, 4, ConfigDef.Width.LONG, SINKS_IDS_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final List<String> sourceIDs;
  public final List<String> storeIDs;
  public final List<String> processorIDs;
  public final List<String> sinkIDs;

  public TopologyConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    sourceIDs = getList(SOURCES_IDS_CONFIG);
    processorIDs = getList(PROCESSORS_IDS_CONFIG);
    storeIDs = getList(STORES_IDS_CONFIG);
    sinkIDs = getList(SINKS_IDS_CONFIG);

    try {
      sourceConfigs = configNodes(sourceIDs, SourceNodeConfig.class);
      processorConfigs = configNodes(processorIDs, ProcessorNodeConfig.class);
      storeConfigs = configNodes(storeIDs, StateStoreConfig.class);
      sinkConfigs = configNodes(sinkIDs, SinkNodeConfig.class);
      List<TopologyNodeConfig> configs = new ArrayList<TopologyNodeConfig>() {{
        addAll(sourceConfigs);
        addAll(processorConfigs);
        addAll(storeConfigs);
        addAll(sinkConfigs);
      }};
      nodeConfigs = configs;

      for (StateStoreConfig s : storeConfigs)
        for (String processorID : s.processors)
          addStore(processorID, s);

    } catch (Exception e) {
      log.error("Failed to create topology configuration");
      throw e;
    }
  }

  private void addStore(String processorID, StateStoreConfig s) {
    ProcessorNodeConfig processorConfig = getNodeConfig(processorID, ProcessorNodeConfig.class);
    if (processorConfig == null)
      throw new ConfigException("Failed to add State Store \""+s.name+"\" to processor \""+processorID+"\" config: processor not found.");
    processorConfig.addStateStore(s);
  }

  private <T extends TopologyNodeConfig> List<T> configNodes(List<String> nodeIDs, Class<T> configClazz) {
    List<T> result = new ArrayList<>();
    for (String nodeID : nodeIDs) {
      try {
        Map<String,Object> nodeProps = originalsWithPrefix(nodeID+".");
        nodeProps.put(TopologyNodeConfig.NAME_CONFIG, nodeID);
        result.add(configClazz.getConstructor(Map.class).newInstance(nodeProps));
      } catch (Exception e) {
        log.error("Failed to configure node \"{}\"", nodeID, e);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public <T extends TopologyNodeConfig> T getNodeConfig(String nodeID, Class<T> clazz) {
    TopologyNodeConfig nodeConfig = null;
    for (TopologyNodeConfig c : nodeConfigs) {
      if (c.getString(TopologyNodeConfig.NAME_CONFIG).equals(nodeID) && clazz.isInstance(c) ) {
        nodeConfig = c;
        break;
      }
    }
    return (T) nodeConfig;
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}

