/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.topology;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;

public class TopologyNodeConfig extends AbstractConfig {

  private static final Logger log = LoggerFactory.getLogger(TopologyNodeConfig.class);

  public enum Type {
    NONE, SOURCE, PROCESSOR, STORE, SINK
  };

  public static final Type NODE_TYPE = Type.NONE;

  public static final String NAME_CONFIG = "id";
  protected static final String NAME_DOC = "Node identifier";
  protected static final String NAME_DISPLAY = "Node id.";

  public static final String NODE_GROUP = "node";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef();
  }

  public final String name;

  public TopologyNodeConfig(ConfigDef config_def, Map<String, String> props) {
    super(config_def, props);
    name = getString(NAME_CONFIG);
  }

  public static void main(String[] args) {
  }
}

