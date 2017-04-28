/**
 * 2017, M Golosova (KIAE)
 *
 **/

package ru.kiae.dkb.kafka.streams.topology;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.common.KafkaException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.lang.Class;

import java.lang.NoSuchMethodException;
import java.lang.SecurityException;
import java.lang.IllegalArgumentException;
import java.lang.ExceptionInInitializerError;
import java.lang.ReflectiveOperationException;

import ru.kiae.dkb.kafka.streams.processor.ProcessorConfig;


public class ProcessorNodeConfig extends TopologyNodeConfig {

  private static final Logger log = LoggerFactory.getLogger(ProcessorNodeConfig.class);

  public static final Type NODE_TYPE = Type.PROCESSOR;

  public static final String PROCESSOR_SUPPLIER_CLASS = "supplier";
  private static final String PROCESSOR_SUPPLIER_DOC = "Processor supplier class";
  private static final String PROCESSOR_SUPPLIER_DISPLAY = "Processor supplier";

  public static final String PROCESSOR_PREFIX = "processor.";


  public static final String PARENTS_CONFIG = "parents";
  private static final String PARENTS_DOC = "(Comma-separated) list of the upstream (source or processor) nodes.";
  private static final String PARENTS_DISPLAY = "Parent nodes";

  public static final String NODE_GROUP = "node";
  public static final String PROCESSOR_GROUP = "processor";
  public static final String TOPOLOGY_GROUP = "topology";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
      // Node
      .define(NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, NAME_DOC,
              NODE_GROUP, 1, ConfigDef.Width.LONG, NAME_DISPLAY)
      // Processor
      .define(PROCESSOR_SUPPLIER_CLASS, ConfigDef.Type.CLASS, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, PROCESSOR_SUPPLIER_DOC,
              PROCESSOR_GROUP, 1, ConfigDef.Width.LONG, PROCESSOR_SUPPLIER_DISPLAY)
      // Topology
      .define(PARENTS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, PARENTS_DOC,
              TOPOLOGY_GROUP, 1, ConfigDef.Width.LONG, PARENTS_DISPLAY);
  }

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public final List<String> parents;
  private Map<String,Object> processorProps;

  public ProcessorNodeConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    parents = getList(PARENTS_CONFIG);
    processorProps = originalsWithPrefix(PROCESSOR_PREFIX);
  }

  public <T> T getInitializedInstance(String key, Class<T> clazz) {
    return getConfiguredInstance(key, clazz);
  }

  public <T> T getInitializedInstance(String key, Class<T> clazz, Map<String, Object> props) {
    if (props == null)
      return getConfiguredInstance(key,clazz);

    Class<?> c = getClass(key);
    if (c == null) return null;
    try {
      Object o = c.getConstructor(Map.class).newInstance(props);
      if (!clazz.isInstance(o))
        throw new KafkaException(c.getName() + " is not an instance of " + clazz.getName());
      return clazz.cast(o);
    } catch (NoSuchMethodException|SecurityException e) {
      throw new KafkaException(c.getName() +" has no public constructor taking one argument of Map<String,Object>", e);
    } catch (IllegalArgumentException|ExceptionInInitializerError|ReflectiveOperationException e) {
      throw new KafkaException("Failed to initialize instance of " + c.getName(), e);
    }
  }

  private List<String> storeIDs = new ArrayList<>();

  public void addStateStore (StateStoreConfig c) {
    if (!storeIDs.contains(c.name))
      storeIDs.add(c.name);
  }

  public Map<String,Object> processorProps() {
    Map<String,Object> result = new HashMap<>();
    result.putAll(processorProps);
    if (storeIDs.size() > 0)
      result.put(ProcessorConfig.STORES_CONFIG, (Object) storeIDs);
    return result;
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}
