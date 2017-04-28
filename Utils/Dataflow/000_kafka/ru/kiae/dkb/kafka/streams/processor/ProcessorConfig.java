/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.processor;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;


public class ProcessorConfig extends AbstractConfig {

  private static final Logger log = LoggerFactory.getLogger(ProcessorConfig.class);

  public static final String STORES_CONFIG = "stores.ids";
  private static final String STORES_DEFAULT = "";
  private static final String STORES_DOC = "(Comma-separated) list of stores used by the processor.";
  private static final String STORES_DISPLAY = "Stores IDs";

  public static final String STORES_GROUP = "stores";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
      // Stores
      .define(STORES_CONFIG, ConfigDef.Type.LIST, STORES_DEFAULT,
              ConfigDef.Importance.MEDIUM, STORES_DOC,
              STORES_GROUP, 1, ConfigDef.Width.LONG, STORES_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final List<String> stores;

  public ProcessorConfig(ConfigDef defaults, Map<String, Object> props) {
    super(defaults, props);
    stores = getList(STORES_CONFIG);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}
