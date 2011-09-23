package net.joshdevins.storm.spout;

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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * This spout can be used to consume messages in a reliable way from a cluster of Kafka brokers. When you create this
 * spout in Storm, it is recommended that the parallelism hint you set be the same as the number of streams you set for
 * the Kafka consumer. This will ensure proper resource utilization and maximal efficiency on the Kafka brokers.
 */
public class KafkaSpout implements IRichSpout {

    private static final long serialVersionUID = 1064554017925026658L;

    private static Logger LOG = Logger.getLogger(KafkaSpout.class);

    private static final Fields OUTPUT_FIELDS = new Fields("payload");

    private final Properties kafkaProperties;

    private final String topic;

    private SpoutOutputCollector collector;

    private ConsumerConnector consumerConnector;

    private ConsumerIterator consumerIterator;

    /**
     * Default constructor. Can only pass in the properties since everything in this class must be serializable!
     */
    public KafkaSpout(final Properties kafkaProperties, final String topic) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    @Override
    public void ack(final Object msgId) {
    }

    @Override
    public void close() {

        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(OUTPUT_FIELDS);
    }

    @Override
    public void fail(final Object msgId) {
    }

    @Override
    public boolean isDistributed() {
        // TODO: this is true in the Kestrel version
        return false;
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
            collector.emit(new Values(msg.payload().array()));

        } else {
            if (LOG.isInfoEnabled()) {
                LOG.info("Message pulled from consumer is null or not valid: " + msg == null ? "null" : "invalid");
            }

            // as per the docs, sleep for a bit
            Utils.sleep(10);
        }
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
            final SpoutOutputCollector collector) {

        this.collector = collector;

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
