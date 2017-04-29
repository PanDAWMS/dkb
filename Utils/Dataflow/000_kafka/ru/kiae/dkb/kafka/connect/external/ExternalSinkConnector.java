/*
 * 2017, M Golosova (KIAE)
 *
 */

package ru.kiae.dkb.kafka.connect.external;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ru.kiae.dkb.kafka.connect.external.sink.ExternalSinkConfig;
import ru.kiae.dkb.kafka.connect.external.sink.ExternalSinkTask;

public final class ExternalSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(ExternalSinkConnector.class);

  private Map<String, String> configProps;

  public Class<? extends Task> taskClass() {
    return ExternalSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = props;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return ExternalSinkConfig.CONFIG_DEF;
  }

  @Override
  public String version() {
//    return Version.getVersion();
     return "0.1";
  }
}
