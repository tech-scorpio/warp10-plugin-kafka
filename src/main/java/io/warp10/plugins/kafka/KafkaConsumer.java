//
//   Copyright 2019-2023  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.plugins.kafka;

import com.google.common.base.Charsets;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.warp.sdk.AbstractWarp10Plugin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class KafkaConsumer {

    public static final String ATTR_CONSUMER = "kafka.consumer";
    public static final String ATTR_SEQNO = "kafka.seqno";

    private static final String PARAM_MACRO = "macro";
    private static final String PARAM_TOPICS = "topics";
    private static final String PARAM_PARALLELISM = "parallelism";
    private static final String LOG_PERIOD_IN_SECONDS = "logperiod";
    private static final String PARAM_TIMEOUT = "timeout";
    private static final String PARAM_CONFIG = "config";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private final AtomicReference<Macro> macro = new AtomicReference<Macro>(null);
    private AtomicLong timeout = new AtomicLong(Long.MAX_VALUE);

    private final AtomicBoolean done = new AtomicBoolean(false);

    private String warpscript;
    private Thread[] executors;
    private MemoryWarpScriptStack stack;

    public KafkaConsumer(Path p) throws Exception {
        LOG.info("INITIALIZING KafkaConsumer " + p);
        //
        // Read content of mc2 file
        //

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        InputStream in = new FileInputStream(p.toFile());
        byte[] buf = new byte[8192];

        try {
            while (true) {
                int len = in.read(buf);
                if (len < 0) {
                    break;
                }
                baos.write(buf, 0, len);
            }
        } finally {
            in.close();
        }

        this.warpscript = new String(baos.toByteArray(), Charsets.UTF_8);
        this.stack = new MemoryWarpScriptStack(null, null, new Properties());
        stack.maxLimits();

        try {
            stack.execMulti(this.warpscript);
        } catch (Throwable t) {
            LOG.error("Caught exception while loading '" + p.getFileName() + "'.", t);
        }

        Object top = stack.pop();

        if (!(top instanceof Map)) {
            throw new RuntimeException("Kafka consumer spec must leave a configuration map on top of the stack.");
        }

        Map<Object, Object> config = (Map<Object, Object>) top;

        //
        // Extract parameters
        //

        this.macro.set((Macro) config.get(PARAM_MACRO));

        // Get topics
        final List<String> topics = new ArrayList<String>();
        List<Object> kafkatopics;
        Pattern pattern = null;

        Object kafkaTopicsParam = config.get(PARAM_TOPICS);

        if (kafkaTopicsParam instanceof List) {
            // If it is a list of Strings
            kafkatopics = (List<Object>) config.get(PARAM_TOPICS);
            for (Object ktopic : kafkatopics) {
                if (!(ktopic instanceof String)) {
                    throw new RuntimeException("Invalid Kafka topic, MUST be a STRING.");
                }
                topics.add(ktopic.toString());
            }
        } else if (kafkaTopicsParam instanceof String) {
            // If it is a Regexp
            try {
                pattern = Pattern.compile((String) kafkaTopicsParam);
            } catch (PatternSyntaxException e) {
                throw new RuntimeException("Invalid Kafka topic regexp: " + e.getMessage());
            }
        } else {
            throw new RuntimeException("Invalid Kafka topic, MUST be a STRING or a List thereof.");
        }

        Map<Object, Object> kafkaconfig = (Map<Object, Object>) config.get(PARAM_CONFIG);

        final Properties configs = new Properties();

        for (Entry<Object, Object> entry : kafkaconfig.entrySet()) {
            if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
                throw new RuntimeException("Invalid Kafka configuration, key and value MUST be STRINGs.");
            }
            configs.put(entry.getKey().toString(), entry.getValue().toString());
        }

        //
        // Force key/value deserializers
        //

        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());

        int parallelism = Integer.parseInt(null != config.get(PARAM_PARALLELISM) ? String.valueOf(config.get(PARAM_PARALLELISM)) : "1");
        int logPeriodInSeconds = Integer.parseInt(null != config.get(LOG_PERIOD_IN_SECONDS) ? String.valueOf(config.get(LOG_PERIOD_IN_SECONDS)) : "0");
        if (config.containsKey(PARAM_TIMEOUT)) {
            this.timeout.set(Long.parseLong(String.valueOf(config.get(PARAM_TIMEOUT))));
        }

        //
        // Create the actual consuming threads
        //

        this.executors = new Thread[parallelism];

        for (int i = 0; i < parallelism; i++) {
            final MemoryWarpScriptStack stck = new MemoryWarpScriptStack(AbstractWarp10Plugin.getExposedStoreClient(), AbstractWarp10Plugin.getExposedDirectoryClient(), new Properties());
            stck.maxLimits();

            //
            // Store the thread index in a stack attribute
            //

            stck.setAttribute(ATTR_SEQNO, i);
            Pattern finalPattern = pattern;
            String clientId = i+"#"+UUID.randomUUID();
            String groupId= (String) configs.get("group.id");
            String groupInstanceId=""+i;
            Thread t = new Thread() {

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
                                long count = 0;
                                List<Map<String, Object>> messages = new ArrayList<>();
                                Set<String> topics = new HashSet<>();
                                for (ConsumerRecord<byte[], byte[]> record : records) {
                                    count++;
                                    Map<String, Object> map = new HashMap<String, Object>();
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
                                //
                                // If no records were received, emit an empty map and call the macro
                                //
                                if (0 == count) {
                                    stck.push(new ArrayList<>());
                                    stck.exec(macro.get());
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
            };

            //
            // We need to set the class loader to the one that was used to load
            // the current class as it might be a specific class loader created by Warp 10
            //

            t.setContextClassLoader(this.getClass().getClassLoader());
            t.setName("Kafka Consumer Thread #" + i + " " + topics.toString());
            t.setDaemon(true);

            this.executors[i] = t;
            t.start();
        }
    }

    public void end() {
        this.done.set(true);
        try {
            for (Thread t : this.executors) {
                t.interrupt();
            }
        } catch (Exception e) {
        }
    }

    public String getWarpScript() {
        return this.warpscript;
    }
}
