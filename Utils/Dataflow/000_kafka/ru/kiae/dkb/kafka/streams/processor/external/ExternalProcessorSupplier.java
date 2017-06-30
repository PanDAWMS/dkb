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

public class ExternalProcessorSupplier implements ProcessorSupplier<String, String> {

    private static final Logger log = LoggerFactory.getLogger(ExternalProcessorSupplier.class);

    private String[] externalCommand;
    private char EOPMarker;
    private final ExternalProcessorConfig config;

    public ExternalProcessorSupplier(Map<String, Object> props) {
            this(new ExternalProcessorConfig(props));
    }

    public ExternalProcessorSupplier(ExternalProcessorConfig config) {
            this.config = config;
            this.externalCommand = config.externalCommand.split(" ");
            this.EOPMarker = config.EOPMarker;
    }

    @Override
    public Processor<String, String> get() {
        return new Processor<String, String>() {
            private ProcessorContext context;
            private Process          externalProcessor;
            private BufferedWriter   externalProcessorSTDIN;
            private BufferedReader   externalProcessorSTDOUT;
            private BufferedReader   externalProcessorSTDERR;

            private Pattern          lmt_p = Pattern.compile(
                                                    "\\(?(TRACE|DEBUG|INFO"
                                                  + "|WARN(?:ING)?|ERROR)\\)?");

            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                this.context = context;
                this.start();
            }

            @Override
            public void process(String dummy, String line) {
              String outline;
              char   outchar;
              int    outcode;
              try {
                externalProcessorSTDIN.write(line);
                externalProcessorSTDIN.newLine();
                externalProcessorSTDIN.flush();
                read_external_stderr();
                outline = externalProcessorSTDOUT.readLine();
                if (EOPMarker != (char) '\n') {
                  while (outline != null) {
                    context.forward(dummy, outline);
                    outcode = externalProcessorSTDOUT.read();
                    if ((char) outcode == EOPMarker) break;
                    else {
                      outchar = (char) outcode;
                      outline = String.valueOf(outchar)+externalProcessorSTDOUT.readLine();
                    }
                  }
                }
                context.forward(dummy, outline);
              }
              catch (IOException e){
                log.error("Failed to read data from external process.");
                read_external_stderr();
                throw new KafkaException(e);
              }
            }

            @Override
            public void punctuate(long timestamp) {
            }

            @Override
            public void close() {
                log.info("Destroying external process.");
                read_external_stderr();
                this.externalProcessor.destroy();
            }

            public void start() {
                try {
                  this.externalProcessor = new ProcessBuilder(externalCommand).start();
                  this.externalProcessorSTDIN  = new BufferedWriter(new OutputStreamWriter(externalProcessor.getOutputStream()));
                  this.externalProcessorSTDOUT = new BufferedReader(new InputStreamReader(externalProcessor.getInputStream()));
                  this.externalProcessorSTDERR = new BufferedReader(new InputStreamReader(externalProcessor.getErrorStream()));
                }
                catch (IOException e){
                  log.error("Can't start new process with command/parameters: {}", externalCommand.toString());
                  throw new KafkaException(e);
                }
            }

            private void read_external_stderr() {
                try {
                    while (externalProcessorSTDERR.ready()) {
                        String line = externalProcessorSTDERR.readLine();
                        this.external_log(line);
                    }
                }
                catch (IOException io) {
                    throw new KafkaException(io);
                }
            }


            private void external_log(String line) {
                Matcher m = lmt_p.matcher(line);
                String type = "TRACE";
                if (m.lookingAt()) {
                    type = m.group(1);
                    line = line.replaceFirst("\\(?" + type + "\\)?", "");
                }
                line = "(EXTERNAL)" + line;
                switch (type) {
                    case "TRACE":
                        log.trace(line);
                        break;
                    case "DEBUG":
                        log.debug(line);
                        break;
                    case "INFO":
                        log.info(line);
                        break;
                    case "WARN":
                    case "WARNING":
                        log.warn(line);
                        break;
                    case "ERROR":
                        log.error(line);
                        break;
                    default:
                        log.trace(line);
                }
            }
        };
    }
}

