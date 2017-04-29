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

public class SinkNodeConfig extends TopologyNodeConfig {

  private static final Logger log = LoggerFactory.getLogger(SinkNodeConfig.class);

  public static final Type NODE_TYPE = Type.SINK;

  public static final String TOPIC_CONFIG = "topic";
  private static final String TOPIC_DOC = "Name of the topic to publish data to";
  private static final String TOPIC_DISPLAY = "Output topic";

  public static final String PARENTS_CONFIG = "parents";
  private static final String PARENTS_DOC = "(Comma-separated) list of the nodes to receive data from";
  private static final String PARENTS_DISPLAY = "Parent nodes";

  public static final String NODE_GROUP = "node";
  public static final String SINK_GROUP = "sink";
  public static final String TOPOLOGY_GROUP = "topology";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
      // Node
      .define(NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, NAME_DOC,
              NODE_GROUP, 1, ConfigDef.Width.LONG, NAME_DISPLAY)
      // Sink
      .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, TOPIC_DOC,
              SINK_GROUP, 1, ConfigDef.Width.LONG, TOPIC_DISPLAY)
      // Topology
      .define(PARENTS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, PARENTS_DOC,
              TOPOLOGY_GROUP, 1, ConfigDef.Width.LONG, PARENTS_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final String topic;
  public final List<String> parents;

  public SinkNodeConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    topic = getString(TOPIC_CONFIG);
    parents = getList(PARENTS_CONFIG);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}

