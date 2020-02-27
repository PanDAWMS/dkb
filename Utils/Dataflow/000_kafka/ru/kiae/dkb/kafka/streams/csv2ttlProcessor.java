/**
 * Kafka streams: first take.
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
 * - understand, how to use this Streaming model for other usecases
 *   in DKB Dataflow
 * - implement stream mode for other Dataflow scripts
 * - extend class csv2ttlProcessor to the general DKB_Processor,
 *   describing all the dataflows between the storages.
 */

package ru.kiae.dkb.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import ru.kiae.dkb.kafka.streams.processor.external.ExternalProcessorSupplier;
import ru.kiae.dkb.kafka.streams.processor.external.ExternalProcessorConfig;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

public class csv2ttlProcessor {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-csv2ttl-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Demo version: every time it reads source from the beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology builder = new Topology();

        builder.addSource("Source", "dataset-metadata-csv");

        Map<String,Object> processorProps = new HashMap<>();
        processorProps.put(ExternalProcessorConfig.EXTERNAL_EOP_MARKER, "'\u0000'");
        processorProps.put(ExternalProcessorConfig.EXTERNAL_COMMAND, "../053_datasets2TTL/csv2sparql.py -m s");
        ExternalProcessorSupplier p = new ExternalProcessorSupplier(processorProps);
        builder.addProcessor("Process-dataset-TTL", p, "Source");
        builder.addSink("Sink-TTL", "dataset-metadata-ttl", "Process-dataset-TTL");

        processorProps.put(ExternalProcessorConfig.EXTERNAL_COMMAND, "../053_datasets2TTL/csv2sparql.py -m s -L");
        p = new ExternalProcessorSupplier(processorProps);
        builder.addProcessor("Process-dataset-SPARQL", p, "Source");
        builder.addSink("Sink-SPARQL", "dataset-metadata-sparql", "Process-dataset-SPARQL");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // If we want just process input and then die.
//        Thread.sleep(5000L);

//        streams.close();
    }
}
