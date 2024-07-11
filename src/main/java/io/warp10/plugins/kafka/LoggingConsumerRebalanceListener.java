package io.warp10.plugins.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class LoggingConsumerRebalanceListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingConsumerRebalanceListener.class);
    private final KafkaConsumer<byte[], byte[]> consumer;

    public LoggingConsumerRebalanceListener(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        consumer.commitSync();
        if (LOG.isWarnEnabled()) {
            LOG.warn("rebalance ! partitions revoked : {}", describe(collection));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        if (LOG.isInfoEnabled()) {
            LOG.info("rebalance ! partitions assigned : {}", describe(collection));
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        org.apache.kafka.clients.consumer.ConsumerRebalanceListener.super.onPartitionsLost(partitions);
        if (LOG.isWarnEnabled()) {
            LOG.warn("rebalance ! partitions lost : {}", describe(partitions));
        }
    }

    private String describe(Collection<TopicPartition> collection) {
        return collection.stream().map(topicPartition -> topicPartition.topic() + "-" + topicPartition.partition()).reduce("", (l, r) -> l + "," + r);
    }
}
