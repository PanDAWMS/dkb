/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.connect.external.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ExternalSourceConfig extends AbstractConfig {

  public static final String EXTERNAL_COMMAND = "external.command";
  private static final String EXTERNAL_COMMAND_DOC = "External command to run as a source process.";
  private static final String EXTERNAL_COMMAND_DISPLAY = "Ext. command";

  public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
  private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data "
                                                     + "(by running the external process).";
  public static final int POLL_INTERVAL_MS_DEFAULT = 86400000; // 1 day
  private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

  public static final String TOPIC_CONFIG = "topic";
  private static final String TOPIC_DOC = "A name of the topic to publish the data to.";
  private static final String TOPIC_DISPLAY = "Topic name";


  public static final String DATA_TYPE_STRING = "string";
  public static final String DATA_TYPE_JSON = "json";
  public static final String DATA_TYPE_JSON_ARRAY = "jsonArray";

  public static final String DATA_TYPE = "data.type";
  private static final String DATA_TYPE_DOC =
      "Input data type. Currently known:"
      + "  * " + DATA_TYPE_STRING + " -- data will be published as-is."
      + "  * " + DATA_TYPE_JSON + " -- data format will be checked and then published as-is."
      + "  * " + DATA_TYPE_JSON_ARRAY + " -- data format will be checked and then published "
      + "    as a set of lines representing JSON entities.";

  public static final String DATA_TYPE_DEFAULT = DATA_TYPE_STRING;
  private static final String DATA_TYPE_DISPLAY = "Data type";


  public static final String DATA_JSON_KEY_FIELD = "data.json.key";
  private static final String DATA_JSON_KEY_DOC = "Field in JSON data to be used as key for partitioning "
      + "and check for doubles. Format: dot-separated list of nested keys.";
  public static final String DATA_JSON_KEY_DEFAULT = (String) null;
  private static final String DATA_JSON_KEY_DISPLAY = "Key field";


  public static final String COMMAND_GROUP = "Command";
  public static final String DATA_GROUP = "Data";
  public static final String CONNECTOR_GROUP = "Connector";


  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
      // External command
      .define(EXTERNAL_COMMAND, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, EXTERNAL_COMMAND_DOC,
              COMMAND_GROUP, 1, ConfigDef.Width.LONG, EXTERNAL_COMMAND_DISPLAY)

      // Data
      .define(DATA_TYPE, ConfigDef.Type.STRING, DATA_TYPE_DEFAULT,
              ConfigDef.Importance.HIGH, DATA_TYPE_DOC,
              DATA_GROUP, 1, ConfigDef.Width.MEDIUM, DATA_TYPE_DISPLAY)
      .define(DATA_JSON_KEY_FIELD, ConfigDef.Type.STRING, DATA_JSON_KEY_DEFAULT,
              ConfigDef.Importance.MEDIUM, DATA_JSON_KEY_DOC,
              DATA_GROUP, 2, ConfigDef.Width.MEDIUM, DATA_JSON_KEY_DISPLAY)

      // Connector
      .define(POLL_INTERVAL_MS_CONFIG, ConfigDef.Type.INT, POLL_INTERVAL_MS_DEFAULT,
              ConfigDef.Importance.HIGH, POLL_INTERVAL_MS_DOC,
              CONNECTOR_GROUP, 1, ConfigDef.Width.SHORT, POLL_INTERVAL_MS_DISPLAY)
      .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, TOPIC_DOC,
              CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM, TOPIC_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final String externalCommand;
  public final String dataType;
  public final int pollIntervalMs;
  public final String topic;
  public final String jsonKeyField;

  public ExternalSourceConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    externalCommand = getString(EXTERNAL_COMMAND);
    dataType = getString(DATA_TYPE);
    pollIntervalMs = getInt(POLL_INTERVAL_MS_CONFIG);
    topic = getString(TOPIC_CONFIG);
    jsonKeyField = getString(DATA_JSON_KEY_FIELD);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}

