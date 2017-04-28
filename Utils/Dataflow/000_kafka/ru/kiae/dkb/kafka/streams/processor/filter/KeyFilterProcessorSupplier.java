/**
 * Kafka streams: Filter processor
 *
 * Filters records by key.
 *
 * NOTE that...
 * ...if we increase number of partitions in the upstream topic, we can't
 *    garanty that we won't let pass some duplicates.
 * ...but it's OK for the case when reprocessing changes nothing
 *
 * TODO
 * - implement BooleanSerde as it's all we need to store as value in StateStore;
 * - maybe we can take information about "currently passed" keys from an
 *   external storage? It would help in case of increasing the number of
 *   partitions.
 */

package ru.kiae.dkb.kafka.streams.processor.filter;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.StateStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;

public class KeyFilterProcessorSupplier implements ProcessorSupplier<String, String> {

  FilterProcessorConfig config;
  String storeName;

  public KeyFilterProcessorSupplier(Map<String,Object> props) {
    this(new FilterProcessorConfig(props));
  }

  public KeyFilterProcessorSupplier(FilterProcessorConfig config) {
    this.config = config;
    List<String> stores = config.stores;
    if (stores.isEmpty())
      throw new RuntimeException("Failed to extract State Storage name from "+String.valueOf(stores));
    String sName = stores.get(0);
    if (sName == null)
      throw new RuntimeException("Failed to extract State Storage name from "+String.valueOf(stores));
    storeName = String.valueOf(sName);
  }

  private static final Logger log = LoggerFactory.getLogger(KeyFilterProcessorSupplier.class);

  @Override
  public Processor<String, String> get() {
    return new Processor<String, String>() {

      private ProcessorContext context;
      // TODO: BooleanSerde
      private KeyValueStore<String, Integer> kvStore;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        StateStore s = context.getStateStore(storeName);
        if (s instanceof KeyValueStore)
          kvStore = (KeyValueStore<String, Integer>) s;
        else
          throw new RuntimeException("Expected "+KeyValueStore.class.getName()+", got "+s.getClass().getName());
      }

      @Override
      public void process(String key, String dummy) {
        if (kvStore.putIfAbsent(key, 1) == null ) {
          context.forward(key, dummy);
        }
      }

      @Override
      public void punctuate(long timestamp) {
      }

      @Override
      public void close() {
        log.info("Closing kvStore");
        this.kvStore.close();
      }
    };
  }
}

