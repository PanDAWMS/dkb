/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.topology;

import org.apache.kafka.common.config.ConfigDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;

public class StateStoreConfig extends TopologyNodeConfig {

  private static final Logger log = LoggerFactory.getLogger(SinkNodeConfig.class);

  public static final Type NODE_TYPE = Type.STORE;

  public static final String KEY_SERDE_CONFIG = "key.serde";
  private static final String KEY_SERDE_DEFAULT = String.class.getName();
  private static final String KEY_SERDE_DOC = "Key serde or data type class.";
  private static final String KEY_SERDE_DISPLAY = "Key serde";

  public static final String VALUE_SERDE_CONFIG = "value.serde";
  private static final String VALUE_SERDE_DEFAULT = String.class.getName();
  private static final String VALUE_SERDE_DOC = "Value serde or data type class.";
  private static final String VALUE_SERDE_DISPLAY = "Value serde";

  public static final String PROCESSORS_CONFIG = "processors";
  private static final String PROCESSORS_DOC = "(Comma-separated) list of the processor nodes using this store.";
  private static final String PROCESSORS_DISPLAY = "Processor nodes";

  public static final String NODE_GROUP = "node";
  public static final String STORE_GROUP = "store";
  public static final String TOPOLOGY_GROUP = "topology";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
      // Node
      .define(NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, NAME_DOC,
              NODE_GROUP, 1, ConfigDef.Width.LONG, NAME_DISPLAY)
      // Store
      .define(KEY_SERDE_CONFIG, ConfigDef.Type.CLASS, KEY_SERDE_DEFAULT,
              ConfigDef.Importance.HIGH, KEY_SERDE_DOC,
              STORE_GROUP, 1, ConfigDef.Width.LONG, KEY_SERDE_DISPLAY)
      .define(VALUE_SERDE_CONFIG, ConfigDef.Type.CLASS, VALUE_SERDE_DEFAULT,
              ConfigDef.Importance.HIGH, VALUE_SERDE_DOC,
              STORE_GROUP, 2, ConfigDef.Width.LONG, VALUE_SERDE_DISPLAY)
      // Topology
      .define(PROCESSORS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, PROCESSORS_DOC,
              TOPOLOGY_GROUP, 1, ConfigDef.Width.LONG, PROCESSORS_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final Class<?> keySerde;
  public final Class<?> valSerde;
  public final List<String> processors;

  public StateStoreConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    keySerde = getClass(KEY_SERDE_CONFIG);
    valSerde = getClass(VALUE_SERDE_CONFIG);
    processors = getList(PROCESSORS_CONFIG);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}
