/**
 * 2017, Golosova M (KIAE)
 *
 * Kafka streams: application creating topologies and running processing nodes.
 *
 */

package ru.kiae.dkb.kafka.streams.cli;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.TopologyBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import ru.kiae.dkb.kafka.streams.topology.TopologyConfig;
import ru.kiae.dkb.kafka.streams.topology.ConfiguredTopologyBuilder;

public class StreamsApplication {

    private static final Logger log = LoggerFactory.getLogger(StreamsApplication.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            log.error("Usage: StreamsApplication application.properties topology1.properties [topology2.properties ...]");
            System.exit(1);
        }

        String appPropsFile = args[0];
        Map<String, String> appProps = !appPropsFile.isEmpty() ?
                Utils.propsToStringMap(Utils.loadProps(appPropsFile)) : Collections.<String, String>emptyMap();

        Properties props = new Properties();
        props.putAll(appProps);

        for (final String topologyPropsFile : Arrays.copyOfRange(args, 1, args.length)) {
          ConfiguredTopologyBuilder builder;
          Map<String, String> topologyProps = !topologyPropsFile.isEmpty() ?
                Utils.propsToStringMap(Utils.loadProps(topologyPropsFile)) : Collections.<String, String>emptyMap();
          TopologyConfig topologyCfg = new TopologyConfig(topologyProps);
          try {
            builder = new ConfiguredTopologyBuilder(topologyCfg);
          } catch (Exception e) {
            log.error("Failed to create ConfiguredTopologyBuilder for {}", topologyPropsFile, e);
            continue;
          }
          log.info("Starting streams for {}", topologyPropsFile);
          KafkaStreams streams = new KafkaStreams((TopologyBuilder) builder, props);
          Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
          }));
          streams.start();
        }

    }
}
