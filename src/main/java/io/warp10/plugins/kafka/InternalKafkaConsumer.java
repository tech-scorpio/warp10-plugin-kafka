package io.warp10.plugins.kafka;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptStack;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static io.warp10.plugins.kafka.KafkaConsumer.ATTR_CONSUMER;

public class InternalKafkaConsumer implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(InternalKafkaConsumer.class);
    private final Properties configs;
    private final String groupId;
    private final String clientId;
    private final String groupInstanceId;
    private final List<String> topics;
    private final Pattern finalPattern;
    private final MemoryWarpScriptStack stck;
    private final AtomicLong timeout;
    private final int logPeriodInSeconds;
    private final AtomicReference<WarpScriptStack.Macro> macro;
    private final AtomicBoolean done = new AtomicBoolean(false);

    public InternalKafkaConsumer(Properties configs,
                                 String groupId,
                                 String clientId,
                                 String groupInstanceId,
                                 List<String> topics,
                                 Pattern finalPattern,
                                 MemoryWarpScriptStack stack,
                                 AtomicLong timeout,
                                 int logPeriodInSeconds,
                                 AtomicReference<WarpScriptStack.Macro> macro) {
        this.configs = configs;
        this.groupId = groupId;
        this.clientId = clientId;
        this.groupInstanceId = groupInstanceId;
        this.topics = topics;
        this.finalPattern = finalPattern;
        this.stck = stack;
        this.timeout = timeout;
        this.logPeriodInSeconds = logPeriodInSeconds;
        this.macro = macro;
    }

    @Override
    public void run() {
        org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer = null;
        while (true) {
            try {
                Properties properties = new Properties(configs);
                properties.put("client.id",clientId);
                properties.put("group.instance.id",groupInstanceId);
                consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(configs);
                if (!topics.isEmpty()) {
                    // subscribes to a list of topics
                    consumer.subscribe(topics,new LoggingConsumerRebalanceListener(consumer));
                } else if (null != finalPattern) {
                    // subscribes to a regular expression
                    consumer.subscribe(finalPattern,new LoggingConsumerRebalanceListener(consumer));
                }

                stck.setAttribute(ATTR_CONSUMER, consumer);

                while (!done.get()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeout.get()));
                    List<Map<String, Object>> messages = new ArrayList<>();
                    Set<String> topics = new HashSet<>();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("timestamp", record.timestamp());
                        map.put("timestampType", record.timestampType().name());
                        map.put("topic", record.topic());
                        map.put("offset", record.offset());
                        map.put("partition", (long) record.partition());
                        map.put("key", record.key());
                        map.put("value", record.value());
                        Map<String, byte[]> headers = new HashMap<>();
                        for (Header header : record.headers()) {
                            headers.put(header.key(), header.value());
                        }
                        map.put("headers", headers);
                        messages.add(map);
                        topics.add(record.topic());
                    }
                    stck.push(messages);
                    long start = System.currentTimeMillis();
                    stck.exec(macro.get());
                    long end = System.currentTimeMillis();
                    long executionTimeInMs = (end-start);
                    String topicsAsString=topics.stream().reduce("",(l,r)->l+","+r);
                    if(LOG.isInfoEnabled()&&!messages.isEmpty()
                            &&(logPeriodInSeconds==0 ||
                            logPeriodInSeconds>0&&((end/1000)%logPeriodInSeconds==0))) {
                        topicsAsString = topicsAsString.substring(1);
                        if(logPeriodInSeconds>0) {
                            LOG.info("{}-{} ({}) :(every {} sec) batchSize:{}, executionTime: {} ms", groupId, groupInstanceId, topicsAsString,logPeriodInSeconds, messages.size(), executionTimeInMs);
                        }else{
                            LOG.info("{}-{} ({}) : batchSize:{}, executionTime: {} ms", groupId, groupInstanceId, topicsAsString, messages.size(), executionTimeInMs);
                        }
                    }

                    consumer.commitAsync();
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Kafka Consumer caught exception ", e);
            } finally {
                if (null != consumer) {
                    try {
                        consumer.close();
                    } catch (Exception e) {
                    }
                }
            }
        }
    }
}
