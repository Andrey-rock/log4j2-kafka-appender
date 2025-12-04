import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin(
        name = "KafkaAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE,
        printObject = true
)
public class KafkaAppender extends AbstractAppender {

    private volatile Producer<String, String> producer;
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
    private int maxBlockMs;

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
            boolean syncSend,
            int maxBlockMs) {

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
        this.maxBlockMs = maxBlockMs;

        LOGGER.info("KafkaAppender '{}' created for topic: {}, brokers: {}",
                name, topic, bootstrapServers);

        initializeProducer();
    }

    private void initializeProducer() {
        try {
            LOGGER.debug("Initializing KafkaProducer for brokers: {}", bootstrapServers);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    keySerializer != null ? keySerializer :
                            "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    valueSerializer != null ? valueSerializer :
                            "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.ACKS_CONFIG, acks != null ? acks : "1");
            props.put(ProducerConfig.RETRIES_CONFIG, retries);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);

            // Важные настройки для логирования
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 2 минуты
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000); // 30 секунд

            // Для отладки
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

            this.producer = new KafkaProducer<>(props);

            LOGGER.info("KafkaProducer initialized successfully for topic: {}", topic);

            // Тест соединения
            testConnection();

        } catch (Exception e) {
            LOGGER.error("Failed to initialize KafkaProducer", e);
            throw new RuntimeException("Failed to initialize KafkaProducer", e);
        }
    }

    private void testConnection() {
        try {
            // Отправляем тестовое сообщение для проверки соединения
            ProducerRecord<String, String> testRecord = new ProducerRecord<>(
                    topic,
                    "__test__",
                    "Test connection message"
            );

            if (syncSend) {
                producer.send(testRecord).get(5, TimeUnit.SECONDS);
            } else {
                producer.send(testRecord, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.warn("Test message failed to send to Kafka: {}", exception.getMessage());
                    } else {
                        LOGGER.debug("Test message sent successfully to partition {} at offset {}",
                                metadata.partition(), metadata.offset());
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.warn("Kafka connection test failed: {}", e.getMessage());
        }
    }

    @Override
    public void append(LogEvent event) {
        if (producer == null) {
            LOGGER.warn("KafkaProducer is not initialized for appender: {}", getName());
            return;
        }

        try {
            String message = new String(getLayout().toByteArray(event));

            // Можно добавить дополнительные поля в сообщение
            String enhancedMessage = String.format("[%s] [%s] %s",
                    event.getLevel(),
                    event.getLoggerName(),
                    message);

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    null, // ключ
                    enhancedMessage
            );

            LOGGER.trace("Sending log to Kafka topic {}: {}", topic, message.substring(0, Math.min(100, message.length())));

            if (syncSend) {
                try {
                    producer.send(record).get(10, TimeUnit.SECONDS);
                    LOGGER.trace("Log sent successfully to Kafka");
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    LOGGER.error("Failed to send log to Kafka synchronously", e);
                }
            } else {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to send log message to Kafka topic {}", topic, exception);
                        // Можно добавить fallback логирование
                    } else {
                        LOGGER.trace("Log sent to Kafka topic {}, partition {}, offset {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("Error in KafkaAppender.append()", e);
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException("Error appending to Kafka", e);
            }
        }
    }

    @Override
    public void start() {
        super.start();
        LOGGER.info("KafkaAppender '{}' started", getName());
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping KafkaAppender '{}'", getName());
        super.stop();

        if (producer != null) {
            try {
                LOGGER.debug("Flushing KafkaProducer...");
                producer.flush();
                LOGGER.debug("Closing KafkaProducer...");
                producer.close();
                LOGGER.info("KafkaProducer closed successfully");
            } catch (Exception e) {
                LOGGER.error("Error closing KafkaProducer", e);
            }
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
            @PluginAttribute("maxBlockMs") String maxBlockMs,
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

        boolean ignore = ignoreExceptions == null ? true : Boolean.parseBoolean(ignoreExceptions);
        int retriesInt = retries != null ? Integer.parseInt(retries) : 3;
        int batchSizeInt = batchSize != null ? Integer.parseInt(batchSize) : 16384;
        int lingerMsInt = lingerMs != null ? Integer.parseInt(lingerMs) : 1;
        int bufferMemoryInt = bufferMemory != null ? Integer.parseInt(bufferMemory) : 33554432;
        boolean syncSendBool = syncSend != null ? Boolean.parseBoolean(syncSend) : false;
        int maxBlockMsInt = maxBlockMs != null ? Integer.parseInt(maxBlockMs) : 60000;

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
                syncSendBool,
                maxBlockMsInt
        );
    }
}