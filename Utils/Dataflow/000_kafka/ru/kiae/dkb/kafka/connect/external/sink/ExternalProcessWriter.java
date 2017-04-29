/*
 * 2017, M Golosova (KIAE)
 *
 */

package ru.kiae.dkb.kafka.connect.external.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.IOException;

import java.lang.InterruptedException;

public class ExternalProcessWriter {

  private static final Logger log = LoggerFactory.getLogger(ExternalProcessWriter.class);

  private final ExternalSinkConfig config;
  private Process externalProcess = null;
  private BufferedWriter externalProcessSTDIN = null;
  private BufferedReader externalProcessSTDOUT = null;
  private BufferedReader externalProcessSTDERR = null;

  // TODO: make things configurable
  // WARNING: BufferedReader.ready() method can't see '\0', so using \6
  private final char EOPMarker = '\6';

  ExternalProcessWriter(final ExternalSinkConfig config) {
    this.config = config;
    try {
      this.externalProcess = Runtime.getRuntime().exec(config.externalCommand);
      this.externalProcessSTDIN  = new BufferedWriter(new OutputStreamWriter(externalProcess.getOutputStream()));
      this.externalProcessSTDOUT = new BufferedReader(new InputStreamReader(externalProcess.getInputStream()));
      this.externalProcessSTDERR = new BufferedReader(new InputStreamReader(externalProcess.getErrorStream()));
    }
    catch (IOException e){
      log.error("Can't start new process with command/parameters: " + config.externalCommand.toString());
    }
  }

  void write(final Collection<SinkRecord> records) throws IOException, InterruptedException {
    String line;
    if (externalProcess == null)
      throw new IOException();
    Integer n = 0;
    for (SinkRecord record : records) {
      n++;
      line = (String) record.value();
      log.trace("Sending line to the external processor: {}", line);
      externalProcessSTDIN.write(line+"\n");
      if (config.batchSize > 0 && n%config.batchSize == 0) {
        log.trace("Record {}: batching.", n.toString());
        externalProcessSTDIN.write("\0\n");
        externalProcessSTDIN.flush();
        waitForResponse();
      }
    }
    if (config.batchSize >= 0)
      externalProcessSTDIN.write("\0\n");
    externalProcessSTDIN.flush();
    waitForResponse();
  }

  private void waitForResponse() throws InterruptedException, IOException {
    int outcode;
    while (!externalProcessSTDOUT.ready()) {
      log.trace("External process is not ready yet. Sleep 100 ms...");
      Thread.sleep(100L);
    }
    outcode = externalProcessSTDOUT.read();
    if ((char) outcode != EOPMarker) {
      throw new RuntimeException("Unexpected code from the external command: "+outcode+" (char: "+ (char)outcode + ")");
    }
  }

  void closeQuietly() {
    log.info("Desroying external process.");
    this.externalProcess.destroy();
  }

}
