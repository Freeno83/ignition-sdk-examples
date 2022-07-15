package com.inductiveautomation.ignition.examples.kafka;

import com.inductiveautomation.ignition.common.alarming.AlarmEvent;
import com.inductiveautomation.ignition.common.alarming.EventData;
import com.inductiveautomation.ignition.examples.kafka.datasink.*;
import com.inductiveautomation.ignition.examples.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.audit.AuditRecord;
import com.inductiveautomation.ignition.gateway.history.HistoryManager;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Written By: Nick Robinson
 * Date: 06-Oct-2021
 * Content:
 *      Setup and removal of sinks
 *      Actions for sending alarm and audit data to Kafka
 */

public class GatewayScriptModule {

    private final Logger logger = LoggerFactory.getLogger("Kafka." + getClass().getSimpleName());

    private HistoryManager historyManager;
    private KafkaSink alarmSink, auditSink, tagSink;
    private GatewayContext context;
    private KafkaSettingsRecord kafkaConfig;
    private Map<String, BaseSink> dataSinksMap = new HashMap<>();

    private final String alarmSignature = "kafka-alarm";
    private final String auditSignature = "kafka-audit";
    private final String alarmSinkName = "kafka-alarm-events";
    private final String auditSinkName = "kafka-audit-events";
    private final String tagSinkName = "kafka-tag-history";
    private  String alarmTopic;
    private  String auditTopic;
    private String hostName;

    public GatewayScriptModule() {
    }

    public void setGatewayContext(GatewayContext ctx) {
        this.context = ctx;
        this.historyManager = ctx.getHistoryManager();
    }

    public Collection<BaseSink> getDataSinks() {
        return this.dataSinksMap.values();
    }

    public void shutDownSinks() {
        dataSinkOperation(SinkOps.Unregister);
        dataSinksMap.clear();
    }

    public void initializeDataSinks(KafkaSettingsRecord kafkaSettings) {
        this.kafkaConfig = kafkaSettings;

        // topics can be changed from the gateway UI
        this.alarmTopic = kafkaSettings.getAlarmsTopic();
        this.auditTopic = kafkaSettings.getAuditTopic();

        shutDownSinks();

        if (kafkaSettings.getEnabled()) {
            this.tagSink = new TagSink(tagSinkName, kafkaSettings);
            this.alarmSink = new KafkaSink(alarmSinkName, kafkaSettings);
            this.auditSink = new KafkaSink(auditSinkName, kafkaSettings);

            this.alarmSink.addStats(alarmSinkName);
            this.auditSink.addStats(auditSinkName);

            this.dataSinksMap.put(tagSinkName, tagSink);
            this.dataSinksMap.put(alarmSinkName, alarmSink);
            this.dataSinksMap.put(auditSinkName, auditSink);

            dataSinkOperation(SinkOps.Register);
        }

        String method = null;

        if (kafkaSettings.getEnabled()) {
            if (kafkaSettings.getUseStoreAndfwd()) {
                method = "sendWithHistoryManager";
            } else method = "sendWithProducer";
        } else method = "doNothing";

        logger.info("Runtime method: " + method);

        try {
            this.hostName = InetAddress.getLocalHost().getHostName().toUpperCase();
        } catch (UnknownHostException e) {
            this.hostName = this.context.getSystemProperties().getSystemName();
        }
    }

    private void dataSinkOperation(SinkOps op) {
        if (op == SinkOps.Register) {
            for (BaseSink sink : this.dataSinksMap.values()) {
                this.historyManager.registerSink(sink);
            }
        }

        if (op == SinkOps.Unregister) {
            for (BaseSink sink : this.dataSinksMap.values()) {
                sink.closeProducer();
                this.historyManager.unregisterSink(sink, true);
            }
        }
    }

    public int sendWithHistoryManager(String sinkName, SinkData data) throws Exception {
        try {
            this.historyManager.storeHistory(sinkName, data);
        } catch (Exception e) {
            logger.error("Error sending data with history manager: " + e.getCause().toString());
        }
        return 0;
    }

    public int sendWithProducer(String sinkName, SinkData data) throws IOException {
        BaseSink sink = this.dataSinksMap.get(sinkName);
        sink.sendPipelineDataWithProducer(data);
        sink.setLastMessageTime(data.getSignature());
        return 0;
    }

    public int doNothing(String sinkName, SinkData data) {
        logger.info("Kafka producer is disabled, data sink inactive: " + sinkName);
        return 2;
    }

    private Boolean nullOrMatchOne(String[] group, String one) {
        return group == null || Arrays.asList(group).stream().anyMatch(i -> one.matches(i.trim()));
    }

    public void sendEquipmentAlarm(AlarmEvent alarm, EventData data, KafkaSettingsRecord kafkaSettings) {

        int minimumPriority = kafkaSettings.getAlarmPriorityInt();
        Boolean hasPriority = alarm.getPriority().ordinal() >= minimumPriority;
        String src = String.valueOf(alarm.getSource()), path = String.valueOf(alarm.getDisplayPath());
        String[] srcFilter = kafkaSettings.getSource(), pathFilter = kafkaSettings.getDispPath(), srcPathFilter = kafkaSettings.getSrcPath();
        Boolean isSource = nullOrMatchOne(srcFilter, src);
        Boolean isPath = nullOrMatchOne(pathFilter, path);
        Boolean isPathOrSource = nullOrMatchOne(srcPathFilter, path) || nullOrMatchOne(srcPathFilter, src);

        String provider = src.split(":")[1];
        String tagPath = src.split("tag:")[1];

        if (hasPriority && isSource && isPath & isPathOrSource) {
            try {
                String json = new JSONObject()
                        .put("uuid", alarm.getId().toString())
                        .put("gatewayName", this.hostName)
                        .put("provider", provider)
                        .put("tagPath", tagPath)
                        .put("displayPath", path)
                        .put("priority", alarm.getPriority().ordinal())
                        .put("eventType", alarm.getState().ordinal())
                        .put("eventData", String.valueOf(data.getRawValueMap()))
                        .put("eventFlags", alarm.getLastEventState().ordinal())
                        .put("epochms", data.getTimestamp())
                        .toString();
                SinkData toSend = new SinkData(alarmTopic, json, alarmSinkName);
                sendKafkaData(toSend, false);
            } catch (JSONException e) {
                logger.error("Error sending alarm data: " + e.getCause());
                this.alarmSink.addOneFailedCount(alarmSignature);
            }
        }
    }

    public void sendAuditData(AuditRecord record) {
        if (kafkaConfig.getAuditEnabled()) {
            long epochms = record.getTimestamp().getTime();
            try {
                String json = new JSONObject()
                        .put("epochms", epochms)
                        .put("originatingSystem", record.getOriginatingSystem())
                        .put("originatingContext", record.getOriginatingContext())
                        .put("actor", record.getActor())
                        .put("actorHost", record.getActorHost())
                        .put("action", record.getAction())
                        .put("actionTarget", record.getActionTarget())
                        .put("actionValue", record.getActionValue())
                        .put("statusCode", record.getStatusCode())
                        .toString();
                SinkData toSend = new SinkData(auditTopic, json, auditSinkName);
                sendKafkaData(toSend, true);
            } catch (JSONException e) {
                logger.error("Error sending audit record: " + e.getCause());
                this.auditSink.addOneFailedCount(auditSignature);
            }
        }
    }

    private void sendKafkaData(SinkData data, Boolean isAudit) {

        String sink = isAudit ? auditSinkName : alarmSinkName;

        try {
            logger.debug("Sending data to Kafka: " + data.toString());

            if (kafkaConfig.getEnabled()) {
                if (kafkaConfig.getUseStoreAndfwd()) {
                    sendWithHistoryManager(sink, data);
                } else {
                    sendWithProducer(sink, data);
                }
            } else {
                doNothing(sink, data);
            }
        } catch (Exception e) {
            logger.error(String.format("Can't send data (%s) due to error: %s", data.toString(), e));

            if (isAudit) {
                this.auditSink.addOneFailedCount(data.getSignature());
            } else {
                this.alarmSink.addOneFailedCount(data.getSignature());
            }
        }
    }
}
