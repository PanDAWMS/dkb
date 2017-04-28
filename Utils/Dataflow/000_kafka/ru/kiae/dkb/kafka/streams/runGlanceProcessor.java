/**
 * Kafka streams: application running GlanceProcessor,
 *
 * which transforms raw GLANCE data into separated records
 * and filters the doubles out.
 * 
 */

package ru.kiae.dkb.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ru.kiae.dkb.kafka.streams.processor.glance.GlanceProcessorSupplier;

public class runGlanceProcessor {

    private static final Logger log = LoggerFactory.getLogger(runGlanceProcessor.class);

    /**
     * This class handles the programs arguments.
     */
    private static class CommandLineValues {
        @Option(name = "-S", aliases = { "--source-topic" }, required = false,
                usage = "source topic")
        private String source="glance-raw";

        @Option(name = "-O", aliases = { "--out-topic" }, required = false,
                usage = "output topic")
        private String out="glance-parsed";

        @Option(name = "-n", aliases = { "--name" }, required = false,
                usage = "processor name")
        private String name="glance-raw-processor";

        @Option(name = "-c", aliases = { "--clean-up" }, required = false,
                usage = "reset application (remove State Store and offsets)")
        private boolean cleanUp=false;
    
        private boolean errorFree = false;
    
        public CommandLineValues(String... args) {
            CmdLineParser parser = new CmdLineParser(this);
            parser.setUsageWidth(80);
            try {
                parser.parseArgument(args);
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
         * Returns application name
         *
         * @return application name.
         */
        public String getName() {
            return name;
        }

        /**
         * Returns --clean-up flag
         *
         * @return --clean-up flag
         */
        public boolean getCleanUp() {
            return cleanUp;
        }
    }

    public static class JsonSerde implements Serde<JsonNode> {
        final private Serializer<JsonNode> serializer;
        final private Deserializer<JsonNode> deserializer;

        public JsonSerde() {
            this.serializer = new JsonSerializer();
            this.deserializer = new JsonDeserializer();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
            deserializer.configure(configs, isKey);
        }

        @Override
        public void close() {
            serializer.close();
            deserializer.close();
        }

        @Override
        public Serializer<JsonNode> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return deserializer;
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

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, values.getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
//        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        // Demo version: every time it reads source from the beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("Source", values.getSource());

        GlanceProcessorSupplier p = new GlanceProcessorSupplier();
        builder.addProcessor("Process", p, "Source");
        StateStoreSupplier kvStore = Stores.create("glance-papers").withIntegerKeys().withValues(new JsonSerde()).inMemory().build();
        builder.addStateStore(kvStore, "Process");
        builder.addSink("Sink", values.getSink(), "Process");

        KafkaStreams streams = new KafkaStreams(builder, props);
        if (values.getCleanUp()) {
          streams.cleanUp();
          System.exit(0);
        }
        streams.start();

        // If we want just process input and then die.
//        Thread.sleep(5000L);

//        streams.close();
    }
}
