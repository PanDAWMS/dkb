/*
 * 2017, M Golosova (KIAE)
 *
 */

package ru.kiae.dkb.kafka.connect.external.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;


public class ExternalSinkConfig extends AbstractConfig {

  public static final String EXTERNAL_COMMAND = "external.command";
  private static final String EXTERNAL_COMMAND_DOC = "External command to run as sink process.";
  private static final String EXTERNAL_COMMAND_DISPLAY = "Ext. command";

  public static final String BATCH_SIZE = "batch.size";
  private static final int BATCH_SIZE_DEFAULT = 100;
  private static final String BATCH_SIZE_DOC =
      "Specifies how many records to attempt to batch together for insertion into the destination table, when possible.";
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";

  public static final String MAX_RETRIES = "max.retries";
  private static final int MAX_RETRIES_DEFAULT = 10;
  private static final String MAX_RETRIES_DOC =
      "The maximum number of times to retry on errors before failing the task.";
  private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
  private static final String RETRY_BACKOFF_MS_DOC =
      "The time in milliseconds to wait following an error before a retry attempt is made.";
  private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

  private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

  private static final String COMMAND_GROUP = "Command";
  private static final String WRITES_GROUP = "Writes";
  private static final String RETRIES_GROUP = "Retries";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      // External command
      .define(EXTERNAL_COMMAND, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, EXTERNAL_COMMAND_DOC,
              COMMAND_GROUP, 1, ConfigDef.Width.LONG, EXTERNAL_COMMAND_DISPLAY)

      // Writes
      .define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
              ConfigDef.Importance.MEDIUM, BATCH_SIZE_DOC,
              WRITES_GROUP, 2, ConfigDef.Width.SHORT, BATCH_SIZE_DISPLAY)

      // Retries
      .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
              ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC,
              RETRIES_GROUP, 1, ConfigDef.Width.SHORT, MAX_RETRIES_DISPLAY)
      .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
              ConfigDef.Importance.MEDIUM, RETRY_BACKOFF_MS_DOC,
              RETRIES_GROUP, 2, ConfigDef.Width.SHORT, RETRY_BACKOFF_MS_DISPLAY);

  public final String externalCommand;
  public final int batchSize;
  public final int maxRetries;
  public final int retryBackoffMs;

  public ExternalSinkConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    externalCommand = getString(EXTERNAL_COMMAND);
    batchSize = getInt(BATCH_SIZE);
    maxRetries = getInt(MAX_RETRIES);
    retryBackoffMs = getInt(RETRY_BACKOFF_MS);
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toRst());
  }

}
