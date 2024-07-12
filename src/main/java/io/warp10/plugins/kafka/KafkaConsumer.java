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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private ExecutorService executorService;

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

        executorService = Executors.newFixedThreadPool(parallelism);
        for (int i = 0; i < parallelism; i++) {
            final MemoryWarpScriptStack stck = new MemoryWarpScriptStack(AbstractWarp10Plugin.getExposedStoreClient(), AbstractWarp10Plugin.getExposedDirectoryClient(), new Properties());
            stck.maxLimits();

            //
            // Store the thread index in a stack attribute
            //

            stck.setAttribute(ATTR_SEQNO, i);
            String clientId = i+"#"+UUID.randomUUID();
            String groupId= (String) configs.get("group.id");
            String groupInstanceId=""+i;

            executorService.submit(new InternalKafkaConsumer(
                    configs,
                    groupId,
                    clientId,
                    groupInstanceId,
                    topics,
                    pattern,
                    stack,
                    timeout,
                    logPeriodInSeconds,
                    macro,
                    done
            ));

        }
    }

    public void end() {
        this.done.set(true);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

    }

    public String getWarpScript() {
        return this.warpscript;
    }
}
