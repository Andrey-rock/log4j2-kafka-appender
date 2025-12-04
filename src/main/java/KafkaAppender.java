import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.Properties;

@Plugin(
        name = "KafkaAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE,
        printObject = true
)
public class KafkaAppender extends AbstractAppender {

    private Producer<String, String> producer;
    private String topic;
    private String bootstrapServers;
    private String keySerializer;
    private String valueSerializer;
    private String acks;
    private int retries;
    private int batchSize;
    private int lingerMs;
    private int bufferMemory;
    private boolean syncSend;

    protected KafkaAppender(
            String name,
            Filter filter,
            Layout<? extends Serializable> layout,
            boolean ignoreExceptions,
            Property[] properties,
            String topic,
            String bootstrapServers,
            String keySerializer,
            String valueSerializer,
            String acks,
            int retries,
            int batchSize,
            int lingerMs,
            int bufferMemory,
            boolean syncSend) {

        super(name, filter, layout, ignoreExceptions, properties);

        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.acks = acks;
        this.retries = retries;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.bufferMemory = bufferMemory;
        this.syncSend = syncSend;

        initializeProducer();
    }

    private void initializeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                keySerializer != null ? keySerializer : "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                valueSerializer != null ? valueSerializer : "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, acks != null ? acks : "1");
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);

        // Оптимизация для логов
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000); // 30 секунд
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 2 минуты

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void append(LogEvent event) {
        try {
            String message = new String(getLayout().toByteArray(event));

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    Thread.currentThread().getName(),
                    message
            );

            if (syncSend) {
                producer.send(record).get();
            } else {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to send log message to Kafka", exception);
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("Error sending log to Kafka", e);
            if (!ignoreExceptions()) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginAttribute("keySerializer") String keySerializer,
            @PluginAttribute("valueSerializer") String valueSerializer,
            @PluginAttribute("acks") String acks,
            @PluginAttribute("retries") String retries,
            @PluginAttribute("batchSize") String batchSize,
            @PluginAttribute("lingerMs") String lingerMs,
            @PluginAttribute("bufferMemory") String bufferMemory,
            @PluginAttribute("syncSend") String syncSend,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") Filter filter,
            @PluginAttribute("ignoreExceptions") String ignoreExceptions) {

        if (name == null) {
            LOGGER.error("No name provided for KafkaAppender");
            return null;
        }

        if (topic == null) {
            LOGGER.error("No topic provided for KafkaAppender");
            return null;
        }

        if (bootstrapServers == null) {
            LOGGER.error("No bootstrapServers provided for KafkaAppender");
            return null;
        }

        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        boolean ignore = ignoreExceptions == null || Boolean.parseBoolean(ignoreExceptions);
        int retriesInt = retries != null ? Integer.parseInt(retries) : 0;
        int batchSizeInt = batchSize != null ? Integer.parseInt(batchSize) : 16384;
        int lingerMsInt = lingerMs != null ? Integer.parseInt(lingerMs) : 1;
        int bufferMemoryInt = bufferMemory != null ? Integer.parseInt(bufferMemory) : 33554432;
        boolean syncSendBool = Boolean.parseBoolean(syncSend);

        return new KafkaAppender(
                name,
                filter,
                layout,
                ignore,
                null,
                topic,
                bootstrapServers,
                keySerializer,
                valueSerializer,
                acks,
                retriesInt,
                batchSizeInt,
                lingerMsInt,
                bufferMemoryInt,
                syncSendBool
        );
    }
}