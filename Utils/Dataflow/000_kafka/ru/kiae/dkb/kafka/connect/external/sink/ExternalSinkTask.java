/*
 * 2017, M Golosova (KIAE)
 *
 */

package ru.kiae.dkb.kafka.connect.external.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.kafka.common.KafkaException;

public class ExternalSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(ExternalSinkTask.class);

  ExternalSinkConfig config;
  private ExternalProcessWriter writer;
  int remainingRetries;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting task");
    config = new ExternalSinkConfig(props);
    remainingRetries = config.maxRetries;
    initWriter();
  }

  void initWriter() {
    log.info("Initializing writer using external command: {}", config.externalCommand);
    writer = new ExternalProcessWriter(config);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Putting them into the external writer...",
              recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
    try {
      writer.write(records);
    } catch (IOException ioe){
      log.error("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries, ioe);
      if (remainingRetries == 0) {
        throw new ConnectException(ioe);
      } else {
        writer.closeQuietly();
        initWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        throw new RetriableException(ioe);
      }
    } catch (InterruptedException inte) {
      log.info("Interrupted by user.");
      stop();
      throw new KafkaException(inte);
    }
    remainingRetries = config.maxRetries;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    log.info("Stopping task");
    writer.closeQuietly();
  }

  @Override
  public String version() {
//    return getClass().getPackage().getImplementationVersion();
    return "0.1";
  }

}
