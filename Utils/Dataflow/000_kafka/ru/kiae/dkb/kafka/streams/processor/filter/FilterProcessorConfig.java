/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.processor.filter;

import ru.kiae.dkb.kafka.streams.processor.ProcessorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;

public class FilterProcessorConfig extends ProcessorConfig {

  private static final Logger log = LoggerFactory.getLogger(ProcessorConfig.class);

  private static final Object STORES_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

  public final String store;

  public FilterProcessorConfig(Map<String, Object> props) {
    super(CONFIG_DEF, props);
    if (stores.size() != 1)
      throw new ConfigException("Must be specified single State Store, get: "+String.valueOf(stores));
    store = stores.get(0);
    if (store == null)
      throw new ConfigException("State Store name can't be NULL.");
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}
