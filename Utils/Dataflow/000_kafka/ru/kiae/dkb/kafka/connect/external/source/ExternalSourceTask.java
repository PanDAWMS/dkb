/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.connect.external.source;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;


public class ExternalSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(ExternalSourceTask.class);

  private Time time;
  private ExternalSourceConfig config;
  private AtomicBoolean stop;
  private ExternalProcessQuerier querier;

  public ExternalSourceTask() {
    this.time = new SystemTime();
  }

  public ExternalSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
//    return Version.getVersion();
    return "0.1";
  }

  @Override
  public void start(Map<String, String> properties) {
    try {
      config = new ExternalSourceConfig(properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start ExternalSourceTask due to configuration error", e);
    }

    initQuerier();
    stop = new AtomicBoolean(false);
  }

  void initQuerier() {
    log.info("Initializing querier using external command: {}", config.externalCommand);
    this.querier = new ExternalProcessQuerier(config);
  }

  @Override
  public void stop() throws ConnectException {
    stop.set(true);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("{} Polling for new data");

    final long sleepMaxMs = 5000; // 5 sec

    while (true) {
      if (!querier.querying()) {
        // If not in the middle of an update, wait for next update time
        final long nextUpdate = querier.getLastUpdate() + config.pollIntervalMs;
        long untilNext = nextUpdate - time.milliseconds();
        log.trace("Waiting {} ms to poll {} next", untilNext, querier.toString());
        long sleepMs = (untilNext < sleepMaxMs) ? untilNext : sleepMaxMs;
        while (!stop.get() && untilNext > 0) {
          log.trace("{} sleeping: {}", querier.toString(), String.valueOf(sleepMs));
          time.sleep(sleepMs);
          untilNext = nextUpdate - time.milliseconds();
          sleepMs = (untilNext < sleepMaxMs) ? untilNext : sleepMaxMs;
          continue; // Re-check stop flag every sleepMaxMs
        }
      }

      if (stop.get()) break;

      final List<SourceRecord> results = new ArrayList<>();
      try {
        log.debug("Checking for next block of results from {}", querier.toString());
        querier.maybeStartQuery();

        List<SourceRecord> records;
        records=querier.extractRecords();
        while ( records.size() > 0 ) {
          results.addAll(records);
          records=querier.extractRecords();
        }

        if (results.isEmpty()) {
          log.trace("No updates for {}", querier.toString());
          continue;
        }

        log.debug("Returning {} records for {}", results.size(), querier.toString());
        return results;
      } catch (IOException e) {
        querier.stopQuery();
        return null;
      }
    }

    // Only in case of shutdown
    return null;
  }

}
