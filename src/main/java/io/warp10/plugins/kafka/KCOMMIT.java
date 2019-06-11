package io.warp10.plugins.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class KCOMMIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public KCOMMIT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    KafkaConsumer<byte[], byte[]> consumer = (KafkaConsumer<byte[],byte[]>) stack.getAttribute(io.warp10.plugins.kafka.KafkaConsumer.ATTR_CONSUMER);
    
    if (null != consumer) {
      consumer.commitSync();
    }
    
    return stack;
  }
}
