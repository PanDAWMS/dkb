/*
 * 2017, M Golosova (KIAE)
 *
 */

package ru.kiae.dkb.kafka.connect.external.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.common.config.ConfigException;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Date;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.InvalidPathException;

/*
 * The External Processor Querier starts an external process, reads its output
 * and transforms the output into a set of records to write into Kafka.
 * Its behaviour depends on the DATA_TYPE:
 *   string:    every line is a message
 *   json:      every line is a JSON
 *   jsonArray: every line is a JSON array
 *
 * TODO: multiline mode with \0 ?
 */
public class ExternalProcessQuerier {

  private static final Logger log = LoggerFactory.getLogger(ExternalProcessQuerier.class);
  private static final Logger timing = LoggerFactory.getLogger("timing");

  private final ExternalSourceConfig config;

  private String[] keyPath = null;
  private String keyName = null;

  private Process externalProcess = null;
  private BufferedReader externalProcessSTDOUT = null;
  private BufferedReader externalProcessSTDERR = null;

  private String programName;
  private Schema valSchema;
  private Schema keySchema;

  private String dataType;
  private Time time;

  protected long lastUpdate;
  protected List<String> resultSet;

  private long starttime;

  ExternalProcessQuerier(final ExternalSourceConfig config) {
    this.config = config;
    if (config.jsonKeyField != null) {
      String[] fullPath = config.jsonKeyField.split("\\.");
      keyPath = Arrays.copyOfRange(fullPath, 0, fullPath.length-1);
      keyName = fullPath[fullPath.length-1];
    }
    lastUpdate = 0;
    time = new SystemTime();
    setProgramName();
    setValSchema();
  }

  private void setProgramName() throws InvalidPathException {
    String command = config.externalCommand;
    String[] splitted = command.split("\\s+",2);
    Path p = Paths.get(splitted[0]);
    programName = p.getFileName().toString();
  }

  private void setValSchema() {
    valSchema = Schema.STRING_SCHEMA;
  }

  private Schema getValSchema() {
    if ( this.valSchema == null ) setValSchema();
    return valSchema;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public boolean querying() {
    return externalProcess != null;
  }

  public void maybeStartQuery() throws IOException {
    if (externalProcess == null) {
      try {
        this.startProcess();
      } catch (IOException e) {
        log.error("Can't start new process with given command/parameters ({}): {}", config.externalCommand, e);
        throw e;
      }
    }
  }

  public void stopQuery() {
    try {
      log.trace("External process exit code: {}", externalProcess.exitValue());
    } catch (IllegalThreadStateException e) {
      log.trace("Terminating external process: {}", config.externalCommand);
      this.externalProcess.destroy();
    }
    externalProcess = null;
    lastUpdate = time.milliseconds();
  }

  private void startProcess() throws IOException {
    starttime = new Date().getTime();
    externalProcess = Runtime.getRuntime().exec(config.externalCommand);
    externalProcessSTDOUT = new BufferedReader(new InputStreamReader(externalProcess.getInputStream()));
    externalProcessSTDERR = new BufferedReader(new InputStreamReader(externalProcess.getErrorStream()));
    timing.info("Source process start took: {} ms", new Date().getTime() - starttime);
  }

  // Returns an array with one line (as it was originally)
  private String extractString() throws IOException {
    String outline = externalProcessSTDOUT.readLine();
    return outline;
  }

  // Returns an array with one line (as it was originally),
  // just checks if it can be parsed as a proper JSON
  private String extractJson() throws IOException {
    String outline = externalProcessSTDOUT.readLine();
    if (outline != null) {
      try {
        JSONObject obj = new JSONObject(outline);
      } catch (JSONException e) {
        String substr = StringUtils.substring(outline, 0, 20);
        if (substr != outline) {
          substr = StringUtils.substring(outline, 0, 17) + "...";
        }
        log.error("Input data is not a valid JSON ({}): {}", substr, e);
        outline = "";
      }
    }
    return outline;
  }

  // Returns an array
  private List<String> extractJsonArray() throws IOException {
    String outline = externalProcessSTDOUT.readLine();
    List<String> results = new ArrayList<>();
    if (outline != null) {
      try {
        JSONArray jsonarray = new JSONArray(outline);
        for (int i = 0; i < jsonarray.length(); i++)
          results.add(jsonarray.getJSONObject(i).toString());
      } catch (JSONException e) {
        String substr = StringUtils.substring(outline, 0, 20);
        if (substr != outline) {
          substr = StringUtils.substring(outline, 0, 17) + "...";
        }
        log.error("Input data is not a valid JSON Array ({}): {}", substr, e);
        results.add("");
      }
    }
    return results;
  }

  private void setKeySchema(Object val) {
    if ( this.keySchema == null ) {
      if ( val == null )
        this.keySchema = null;
      else {
        Schema.Type t = ConnectSchema.schemaType(val.getClass());
        this.keySchema = new ConnectSchema(t);
        log.info("Detected key schema type: {}", t.name());
      }
    }
  }

  private Schema getKeySchema(Object val) {
    if ( this.keySchema == null ) setKeySchema(val);
    return this.keySchema;
  }

  private Schema getKeySchema() {
    return getKeySchema(null);
  }

  private Object getKeyValue(String record) throws JSONException {
    JSONObject cur = new JSONObject(record);
    for (String key : keyPath) {
      cur = cur.getJSONObject(key);
    }
    Object val = cur.get(keyName);
    return val;
  }

// Extracts 1 item (line, JSON line, ...) from the input stream.
// It can be transformed into one or more records, depending on the data type.
  public List<SourceRecord> extractRecords() throws IOException {
    long start = new Date().getTime();
    log.error("Source process extract records: {} since start.", start - starttime);
    List<SourceRecord> result = new ArrayList<>();
    List<String> records = new ArrayList<>();
    String record = null;
    Object key = null;
    final Map<String, String> srcPartition;
    boolean tryAgain = true;
    while (tryAgain) {
      switch (config.dataType) {
        case ExternalSourceConfig.DATA_TYPE_STRING:
          record = extractString();
          if (record == "") continue;
          if (record == null) {
            tryAgain = false;
            continue;
          }
          records.add(record);
          break;
        case ExternalSourceConfig.DATA_TYPE_JSON:
          record = extractJson();
          if (record == "") continue;
          if (record == null) {
            tryAgain = false;
            continue;
          }
          records.add(record);
          break;
        case ExternalSourceConfig.DATA_TYPE_JSON_ARRAY:
          records = extractJsonArray();
          if (records.size() == 0) {
            tryAgain = false;
            continue;
          }
          if (records.get(0) == "") continue;
          break;
        default:
          throw new ConfigException("Invalid data type: " + config.dataType);
      }
      tryAgain=false;
    }
    srcPartition = Collections.singletonMap("program", programName);

    for (String r : records) {
      if (keyName != null) {
        try {
          key = getKeyValue(r);
        } catch (JSONException e) {
          log.error("Failed to extract key ({}) for the record: {}", config.jsonKeyField, r, e);
        }
      }
//                               (srcPartition, srcOffset, topic,        keySchema,         key, valSchema,      value)
      result.add(new SourceRecord(srcPartition, null,      config.topic, getKeySchema(key), key, getValSchema(), (Object) r));
    }
    if ( result.size() == 0 ) stopQuery();
    else {
        long stop = new Date().getTime();
        timing.info("Source process extract of {} records took: {} ms (avg: {} ms).", result.size(), stop - start, (stop-start) / result.size());
    }
    return result;
  }

}
