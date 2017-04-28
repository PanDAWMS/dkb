/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.processor.external;

import ru.kiae.dkb.kafka.streams.processor.ProcessorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ExternalProcessorConfig extends ProcessorConfig {

  private static final Logger log = LoggerFactory.getLogger(ExternalProcessorConfig.class);

  public static final String EXTERNAL_COMMAND = "external.command";
  private static final String EXTERNAL_COMMAND_DOC = "External command to run as a processor.";
  private static final String EXTERNAL_COMMAND_DISPLAY = "Ext. command";

  public static final String EXTERNAL_EOP_MARKER = "eop.marker";
  private static final String EXTERNAL_EOP_MARKER_DEFAULT = "'\n'";
  private static final String EXTERNAL_EOP_MARKER_DOC = "Character marker indicating the end of processing. "
    + "To be used for cases when the external processor produces a number of output messages for one input."
    + "'\\n' default marker means that external processor produces exactly one output message for any input one." ;
  private static final String EXTERNAL_EOP_MARKER_DISPLAY = "EOP marker";

  public static final String EXTERNAL_GROUP = "external";

  public static ConfigDef baseConfigDef() {
    return ProcessorConfig.baseConfigDef()
      // External
      .define(EXTERNAL_COMMAND, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, EXTERNAL_COMMAND_DOC,
              EXTERNAL_GROUP, 1, ConfigDef.Width.LONG, EXTERNAL_COMMAND_DISPLAY)
      .define(EXTERNAL_EOP_MARKER, ConfigDef.Type.STRING, EXTERNAL_EOP_MARKER_DEFAULT,
              ConfigDef.Importance.HIGH, EXTERNAL_EOP_MARKER_DOC,
              EXTERNAL_GROUP, 2, ConfigDef.Width.LONG, EXTERNAL_EOP_MARKER_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final String externalCommand;
  public final char EOPMarker;

  public ExternalProcessorConfig(Map<String, Object> props) {
    super(CONFIG_DEF, props);
    externalCommand = getString(EXTERNAL_COMMAND);
    String stringEOPMarker = getString(EXTERNAL_EOP_MARKER).replaceAll("^\"|^\'|\"$|\'$", "");
    stringEOPMarker = stringEOPMarker.replaceAll("^\"|^\'|\"$|\'$", "");
    if (stringEOPMarker.length() == 1)
      EOPMarker = stringEOPMarker.charAt(0);
    else
      throw new ConfigException("output.delimiter must consist of one character, get "+String.valueOf(stringEOPMarker.length())+" (\'" +stringEOPMarker+"\'). Use quotes and \\uXXXX notation if necessary.");
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}

