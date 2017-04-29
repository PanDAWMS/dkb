/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.connect.external;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ru.kiae.dkb.kafka.connect.external.source.ExternalSourceConfig;
import ru.kiae.dkb.kafka.connect.external.source.ExternalSourceTask;

public class ExternalSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(ExternalSourceConnector.class);

  private Map<String, String> configProperties;
  private ExternalSourceConfig config;

  @Override
  public void start(Map<String, String> properties) throws ConnectException {
    try {
      configProperties = properties;
      config = new ExternalSourceConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start ExternalSourceConnector due to configuration "
                                 + "error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return ExternalSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProperties);
    }
    return configs;
  }

  @Override
  public void stop() throws ConnectException {
  }

  @Override
  public ConfigDef config() {
    return ExternalSourceConfig.CONFIG_DEF;
  }

  @Override
  public String version() {
//    return Version.getVersion();
      return "0.1";
  }
}
