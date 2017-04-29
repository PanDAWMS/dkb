/**
 * Kafka streams: application running pipe (file) connector.
 *
 * 
 */

package ru.kiae.dkb.kafka.connect;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

// For Kafka >= 0.10.1.0
import org.apache.kafka.connect.runtime.ConnectorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.net.URI;

import java.io.File;
import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class runPipeConnector {

    private static final Logger log = LoggerFactory.getLogger(runPipeConnector.class);

    /**
     * This class handles the programs arguments.
     */
    private static class CommandLineValues {
        @Option(name = "-S", aliases = { "--source" }, required = true,
                usage = "source pipe name")
        private String source;

        @Option(name = "-O", aliases = { "--out-topic" }, required = true,
                usage = "output topic")
        private String out;

        @Option(name = "-c", aliases = { "--command" }, required = false,
                usage = "external command to run (with parameters) to write data into pipe. The command must be able to understand parameter --pipe AND take care of producing data continuously.")
        private String command=null;
    
        @Option(name = "-n", aliases = { "--name" }, required = false,
                usage = "processor name")
        private String name;
    
        private boolean errorFree = false;
    
        public CommandLineValues(String... args) {
            CmdLineParser parser = new CmdLineParser(this);
            parser.setUsageWidth(80);
            try {
                parser.parseArgument(args);

                if (command != null) {
                  File f=new File(getCommand()[0]);
    
                  if (!f.isFile()) {
                      throw new CmdLineException(parser,
                            "--command is not a file.");
                  }
                  if (!f.canExecute()) {
                      throw new CmdLineException(parser,
                            "--command is not an executable file.");
                  }
                }
    
                errorFree = true;
            } catch (CmdLineException e) {
                log.error("Failed to parse command line parameters.", e);
                parser.printUsage(System.err);
            }
        }
    
        /**
         * Returns whether the parameters could be parsed without an
         * error.
         *
         * @return true if no error occurred.
         */
        public boolean isErrorFree() {
            return errorFree;
        }
    
        /**
         * Returns splitted command name with arguments.
         *
         * @return Splitted command name with arguments.
         */
        public String[] getCommand() {
            if (command != null)
              return command.split(" ");
            else
              return null;
        }

        /**
         * Returns source topic name.
         *
         * @return source topic name.
         */
        public String getSource() {
            return source;
        }

        /**
         * Returns sink topic name.
         *
         * @return sink topic name.
         */
        public String getSink() {
            return out;
        }

        /**
         * Returns application name or external command name.
         *
         * @return application name.
         */
        public String getName() {
            String r = name;
            if (r == null && getCommand() != null) r = new File(getCommand()[0]).getName();
            return r;
        }

    }

    public static void runExternal(String[] command, String pipename) {
      String[] extendedCommand = new String[command.length+2];
      System.arraycopy(command,0, extendedCommand,0, command.length);
      System.arraycopy(new String[] {"--pipe", pipename}, 0, extendedCommand, command.length, 2);
      try {
        Process externalProcessor = new ProcessBuilder(extendedCommand).start();
      }
      catch (IOException e){
        log.error("Can't start new process with command/parameters: {}", Arrays.toString(extendedCommand), e);
        System.exit(1);
      }

    }

    public static void main(String[] args) throws Exception {

        CommandLineValues values = new CommandLineValues(args);
        CmdLineParser parser = new CmdLineParser(values);
    
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.exit(1);
        }

        // First run external command, if specified.
        if ( values.getCommand() != null )
          runExternal(values.getCommand(), values.getSource());

        // Next create listener
        Map<String, String> workerProps = new HashMap<String,String>();
        workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        workerProps.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "10000");
//        workerProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
//        workerProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
//        workerProps.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
//        workerProps.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.put("internal.key.converter.schemas.enable","false");
        workerProps.put("internal.value.converter.schemas.enable","false");
        workerProps.put("key.converter.schemas.enable","false");
        workerProps.put("value.converter.schemas.enable","false");
        workerProps.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/dev/null");

        StandaloneConfig config = new StandaloneConfig(workerProps);
        RestServer rest = new RestServer(config);
        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        Time time = new SystemTime();
        Worker worker;

// For Kafka >= 0.10.1.0
        ConnectorFactory connectorFactory = new ConnectorFactory();
        worker = new Worker(workerId, time, connectorFactory, config, new FileOffsetBackingStore());

// For Kafka < 0.10.1.0
//        worker = new Worker(workerId, time, config, new FileOffsetBackingStore());

        Herder herder = new StandaloneHerder(worker);
        final Connect connect = new Connect(herder, rest);

        Map<String, String> connectorProps = new HashMap<String,String>();

        connectorProps.put(ConnectorConfig.NAME_CONFIG, values.getName());
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceConnector");
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        connectorProps.put(FileStreamSourceConnector.TOPIC_CONFIG, values.getSink());
        connectorProps.put(FileStreamSourceConnector.FILE_CONFIG, values.getSource());

        try {
            connect.start();
            FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
                    @Override
                    public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                        if (error != null)
                            log.error("Failed to create job: {}", info.result().name());
                        else
                            log.info("Created connector: {}", info.result().name());
                    }
                });
                herder.putConnectorConfig(
                        connectorProps.get(ConnectorConfig.NAME_CONFIG),
                        connectorProps, false, cb);
                cb.get();
        } catch (Throwable t) {
            log.error("Stopping after connector error", t);
            connect.stop();
        }

        // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
        connect.awaitStop();
    }
}
