package com.inductiveautomation.ignition.examples.kafka.datasink;

import com.inductiveautomation.ignition.examples.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Written By: Nick Robinson
 * Date: 05-Oct-2021
 * Content: kafka producer and SSL settings, actual sending of data to Kafka
 */

public class KafkaSink extends BaseSink{

    private static GatewayContext context;
    private static Logger logger = LoggerFactory.getLogger("Kafka");
    private KafkaProducer<String, String> producer;

    public KafkaSink(String pipelineName, KafkaSettingsRecord kafkaSettings) {
        super(pipelineName);
        this.config = kafkaSettings;
        resetProducer(kafkaSettings);
    }

    public void addStats(String signature) {
        this.stats.put(signature, new MessageStats(signature));
    }

    @Override
    public void sendDataWithProducer(SinkData value) throws IOException {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(value.getTopic(), value.getValue());
        sendProducerData(value.getSignature(), record);
    }

    @Override
    public void sendPipelineDataWithProducer(SinkData data) throws IOException {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(data.getTopic(), data.getValue());
        sendProducerData(data.getSignature(), record);
    }

    private void sendProducerData(String signature, ProducerRecord<String, String> record) {
        try {
            this.producer.send(record);
            this.addOneSuccessCount(signature);
        } catch (Exception e) {
            logger.warn(String.format("Data (%s) was not sent due to error: %s", String.valueOf(record), e.toString()));
            this.addOneFailedCount(signature);
        }
    }

    public void resetProducer(KafkaSettingsRecord kafkaSettings) {
        Thread.currentThread().setContextClassLoader(null); //Testing from stack overflow
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSettings.getBrokerList());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer settings
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        if (kafkaSettings.getUseSSL()){
            this.producer = new KafkaProducer<String, String>(getSSLProps(props));
        } else {
            this.producer = new KafkaProducer<String, String>(props);
        }
        this.resetStats();
    }

    private Properties getSSLProps(Properties props){
        String homePath = getGatewayHome();
        String sep = File.separator;

        logger.debug("homepath = " + homePath);
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG,"SSL");
        props.put("security.protocol","SSL");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,"");

        // Keystore settings
        String keystorePath = homePath+String.format("%swebserver%sssl.pfx",sep,sep,sep);
        logger.debug("SSL Keystore Path: " + keystorePath);

        if(fileExists(keystorePath)) {
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePwd);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,keyStorePwd);
        }

        // Truststore settings
        String truststorePath = homePath+String.format("%sdata%scertificates%struststore.jks",sep,sep,sep);
        logger.debug("SSL TrustStore Path: " + truststorePath);

        if (fileExists(truststorePath)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePwd);
        }
        return props;
    }

    private static String getGatewayHome(){
        String absPath = context.getSystemManager().getDataDir().getAbsolutePath();
        return absPath.substring(0,absPath.lastIndexOf(File.separator));
    }

    private static boolean fileExists(String path){
        File fObj = new File(path);
        return fObj.exists();
    }

    @Override
    public void closeProducer() {
        if (this.producer != null) {
            logger.info("Closing producer for sink " + getPipelineName());
            this.producer.close();
        }
    }
}
