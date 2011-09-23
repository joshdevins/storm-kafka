package net.joshdevins.storm.spout;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.utils.Utils;

/**
 * This spout can be used to consume messages in a reliable way from a cluster of Kafka brokers. When you create this
 * spout in Storm, it is recommended that the parallelism hint you set be the same as the number of streams you set for
 * the Kafka consumer. This will ensure proper resource utilization and maximal efficiency on the Kafka brokers.
 * 
 * @author Josh Devins
 */
public class KafkaSpout extends BasicSchemeSpout {

    private static final long serialVersionUID = 1064554017925026658L;

    private static Logger LOG = Logger.getLogger(KafkaSpout.class);

    private final Properties kafkaProperties;

    private final String topic;

    private ConsumerConnector consumerConnector;

    private ConsumerIterator consumerIterator;

    /**
     * Default constructor. Can only pass in the properties since everything in this class must be serializable!
     */
    public KafkaSpout(final Properties kafkaProperties, final String topic, final Scheme scheme) {
        super(scheme);

        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    @Override
    public void close() {

        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }

    @Override
    public boolean isDistributed() {
        // TODO: this is true in the Kestrel version
        return super.isDistributed();
    }

    @Override
    public void nextTuple() {

        if (!consumerIterator.hasNext()) {

            // as per the docs, sleep for a bit
            Utils.sleep(50);
            return;
        }

        Message msg = consumerIterator.next();

        if (msg != null && msg.isValid()) {

            // get the trimmed byte array given the size of the message payload
            ByteBuffer byteBuffer = msg.payload();
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);

            emit(bytes);

        } else {
            if (LOG.isInfoEnabled()) {
                LOG.info("Message pulled from consumer is null or not valid: " + msg == null ? "null" : "invalid");
            }

            // as per the docs, sleep for a bit
            Utils.sleep(10);
        }
    }

    @Override
    public void open() {

        // build up the Kafka consumer
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        Map<String, List<KafkaMessageStream>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaMessageStream stream = consumerMap.get(topic).get(0);

        consumerIterator = stream.iterator();
    }

}
