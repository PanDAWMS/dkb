/**
 * Kafka streams: GLANCE processor supplier
 * 
 * GlanceProcessor transforms raw GLANCE data into separated records.
 * 
 */

package ru.kiae.dkb.kafka.streams.processor.glance;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GlanceProcessorSupplier implements ProcessorSupplier<JsonNode, JsonNode> {

    private static final Logger log = LoggerFactory.getLogger(GlanceProcessorSupplier.class);

    @Override
    public Processor<JsonNode, JsonNode> get() {
        return new Processor<JsonNode, JsonNode>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, JsonNode> kvStore;

            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                this.context = context;
                this.kvStore = (KeyValueStore<Integer, JsonNode>) context.getStateStore("glance-papers");
            }

            @Override
            public void process(JsonNode dummy, JsonNode line) {
                Integer key;
                String GlanceID;
                JsonNode oldValue;
                if (line.isArray())
                  for (final JsonNode node : line) {
                      GlanceID=node.get("id").textValue();
                      key = Integer.parseInt(GlanceID);
                      oldValue = this.kvStore.get(key);
                      if ( node.equals(oldValue) )
                        continue;
                      this.kvStore.put(key,node);
                      context.forward(dummy,node);
                  }
                else
                    log.warn("Input line is not an array. Skipping: {}", line.toString());
            }

            @Override
            public void punctuate(long timestamp) {
            }

            @Override
            public void close() {
               this.kvStore.close();
            }

        };
    }
}

