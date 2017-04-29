/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.topology;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;


public class SourceNodeConfig extends TopologyNodeConfig {

  private static final Logger log = LoggerFactory.getLogger(SourceNodeConfig.class);

  public static final Type NODE_TYPE = Type.SOURCE;

  public static final String TOPICS_CONFIG = "topics";
  private static final String TOPICS_DOC = "(Comma-separated) list of the source topics";
  private static final String TOPICS_DISPLAY = "Source topics";

  public static final String NODE_GROUP = "node";
  public static final String SOURCE_GROUP = "source";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
      // Node
      .define(NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, NAME_DOC,
              NODE_GROUP, 1, ConfigDef.Width.LONG, NAME_DISPLAY)
      // Source
      .define(TOPICS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, TOPICS_DOC,
              SOURCE_GROUP, 1, ConfigDef.Width.LONG, TOPICS_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final List<String> topics;

  public SourceNodeConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    topics = getList(TOPICS_CONFIG);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}

