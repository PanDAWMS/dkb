/**
 * Kafka streams: application running ExternalProcessor.
 *
 * 
 */

package ru.kiae.dkb.kafka.streams;

import org.apache.kafka.common.utils.Utils;

import org.apache.kafka.streams.KafkaStreams;

import ru.kiae.dkb.kafka.streams.topology.TopologyConfig;
import ru.kiae.dkb.kafka.streams.topology.ProcessorNodeConfig;
import ru.kiae.dkb.kafka.streams.topology.SinkNodeConfig;
import ru.kiae.dkb.kafka.streams.topology.ConfiguredTopologyBuilder;
import ru.kiae.dkb.kafka.streams.processor.external.ExternalProcessorSupplier;
import ru.kiae.dkb.kafka.streams.processor.external.ExternalProcessorConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;
import java.util.Collections;

public class runExternalProcessor {

    private static final Logger log = LoggerFactory.getLogger(runExternalProcessor.class);

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            log.error("Usage: runExternalProcessor application.properties plain-topology.properties");
            System.exit(1);
        }

        String appPropsFile = args[0];
        Map<String, String> appProps = !appPropsFile.isEmpty() ?
                Utils.propsToStringMap(Utils.loadProps(appPropsFile)) : Collections.<String, String>emptyMap();

        Properties props = new Properties();
        props.putAll(appProps);

        String topologyPropsFile = args[1];
        Map<String, String> topologyProps = !topologyPropsFile.isEmpty() ?
                Utils.propsToStringMap(Utils.loadProps(topologyPropsFile)) : Collections.<String, String>emptyMap();

        /* Creating plain topology:
         * source -> process(ExternalProcessor) -> sink
         */
        final String sourceID = "source";
        final String processorID = "process";
        final String sinkID = "sink";

        topologyProps.put(TopologyConfig.SOURCES_IDS_CONFIG,sourceID);
        topologyProps.put(TopologyConfig.PROCESSORS_IDS_CONFIG,processorID);
        topologyProps.put(processorID + "." + ProcessorNodeConfig.PARENTS_CONFIG,sourceID);
        topologyProps.put(processorID + "." + ProcessorNodeConfig.PROCESSOR_SUPPLIER_CLASS, ExternalProcessorSupplier.class.getName());
        topologyProps.put(TopologyConfig.SINKS_IDS_CONFIG,sinkID);
        topologyProps.put(sinkID + "." + SinkNodeConfig.PARENTS_CONFIG,processorID);

        TopologyConfig topologyCfg = new TopologyConfig(topologyProps);

        ConfiguredTopologyBuilder builder = new ConfiguredTopologyBuilder(topologyCfg);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // If we want just process input and then die.
//        Thread.sleep(5000L);

//        streams.close();
    }
}
