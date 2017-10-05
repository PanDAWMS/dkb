/**
 * Kafka streams: External processor
 * 
 * Realization of Processor, which can use any external programm as a processing
 * pipe.
 * 
 * NOTE that...
 * ...it takes one line as a message. So don't try to break your output in
 *    multiple lines for readability.
 * ...it takes '\0' as End Of Processing signal from the external programm.
 * ...two Processors (Process-dataset-TTL, Process-dataset-SPARQL) are not
 *    actually parallel: if one freeze on input line, the another won't take
 *    next line until the first is unfrozen.
 * 
 * TODO
 * - multiline messages
 */

package ru.kiae.dkb.kafka.streams.processor.external;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.common.KafkaException;

import ru.kiae.dkb.kafka.common.external.ExternalProcessLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Date;

import java.lang.IllegalThreadStateException;

public class ExternalProcessorSupplier implements ProcessorSupplier<String, String> {

    private static final Logger log = LoggerFactory.getLogger(ExternalProcessorSupplier.class);
    private static final Logger timing = LoggerFactory.getLogger("timing");

    private String[] externalCommand;
    private char EOPMarker;
    private char EOMMarker;
    private final ExternalProcessorConfig config;

    public ExternalProcessorSupplier(Map<String, Object> props) {
            this(new ExternalProcessorConfig(props));
    }

    public ExternalProcessorSupplier(ExternalProcessorConfig config) {
            this.config = config;
            this.externalCommand = config.externalCommand.split(" ");
            this.EOPMarker = config.EOPMarker;
            this.EOMMarker = '\n';
    }

    @Override
    public Processor<String, String> get() {
        return new Processor<String, String>() {
            private ProcessorContext context;
            private Process          externalProcessor;
            private BufferedWriter   externalProcessorSTDIN;
            private BufferedReader   externalProcessorSTDOUT;
            private ExternalProcessLogger externalProcessorLogger;

            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                long start = new Date().getTime();
                this.context = context;
                this.start();
                timing.info("({}) Processor initialization took: {} ms", externalCommand[0], new Date().getTime() - start);
            }

            @Override
            public void process(String dummy, String line) {
              long processing_start = new Date().getTime();
              long start = processing_start;
              long time = 0L;
              String outline;
              char   outchar;
              int    outcode;
              try {
                externalProcessorSTDIN.write(line);
                externalProcessorSTDIN.newLine();
                externalProcessorSTDIN.flush();
                while (! externalProcessorSTDOUT.ready())
                   Thread.sleep(1000);
                StringBuilder buf = new StringBuilder(256);
                outcode = externalProcessorSTDOUT.read();
                while (outcode != -1) {
                    outchar = (char) outcode;
                    if (outchar == EOPMarker || outchar == EOMMarker) {
                        if (buf.length() > 0) {
                            time += new Date().getTime() - start;
                            context.forward(dummy,buf.toString());
                            start = new Date().getTime();
                            buf.setLength(0);
                            buf.trimToSize();
                        }
                        if (outchar == EOPMarker) break;
                    }
                    else
                        buf.append(outchar);
                    outcode = externalProcessorSTDOUT.read();
                }
                if (outcode == -1)
                    throw new KafkaException("External process seems to be dead.");
                log.debug("Processing finished.");
              }
              catch (IOException e){
                log.error("Failed to read data from external process (" + externalCommand[0] + ").");
                try {
                    log.error("Exit code: " + this.externalProcessor.exitValue());
                } catch (IllegalThreadStateException ill) {
                    log.error("Though the external process is still alive.");
                }
                throw new KafkaException(e);
              }
              catch (InterruptedException int_e) {
                log.error("Interrupted by user.");
                throw new KafkaException(int_e);
              }
              time += new Date().getTime() - start;
              timing.info("({}) Message processing took: {} ms", externalCommand[0], time);
              timing.info("({}) Full processing took: {} ms", externalCommand[0], new Date().getTime() - processing_start);
            }

            @Override
            public void punctuate(long timestamp) {
            }

            @Override
            public void close() {
                log.info("Destroying external process.");
                this.externalProcessor.destroy();
            }

            public void start() {
                try {
                  this.externalProcessor = new ProcessBuilder(externalCommand).start();
                  this.externalProcessorSTDIN  = new BufferedWriter(new OutputStreamWriter(externalProcessor.getOutputStream()));
                  this.externalProcessorSTDOUT = new BufferedReader(new InputStreamReader(externalProcessor.getInputStream()));
                  this.externalProcessorLogger = new ExternalProcessLogger(externalProcessor, externalCommand[0]);
                  (new Thread(this.externalProcessorLogger)).start();
                }
                catch (IOException e){
                  log.error("Can't start new process with command: {}", externalCommand[0]);
                  throw new KafkaException(e);
                }
            }
        };
    }
}

