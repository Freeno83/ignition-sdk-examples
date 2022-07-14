package com.inductiveautomation.ignition.examples.kafka.records;

import com.google.common.base.Strings;
import org.json.JSONException;
import org.json.JSONObject;
import simpleorm.dataset.SFieldFlags;
import com.inductiveautomation.ignition.gateway.localdb.persistence.BooleanField;
import com.inductiveautomation.ignition.gateway.localdb.persistence.Category;
import com.inductiveautomation.ignition.gateway.localdb.persistence.IdentityField;
import com.inductiveautomation.ignition.gateway.localdb.persistence.PersistentRecord;
import com.inductiveautomation.ignition.gateway.localdb.persistence.RecordMeta;
import com.inductiveautomation.ignition.gateway.localdb.persistence.StringField;

/**
 * Written By: Nick Robinson
 * Date: 06-Oct-2021
 * Content:  Definitions for the persistence table (SQL Lite table) and accessor methods
 */

public class KafkaSettingsRecord extends PersistentRecord {

    public static final RecordMeta<KafkaSettingsRecord> META = new RecordMeta<KafkaSettingsRecord>(
            KafkaSettingsRecord.class, "KafkaSettingsRecord").setNounKey("KafkaSettingsRecord.Noun").setNounPluralKey(
            "KafkaSettingsRecord.Noun.Plural");

    public static final IdentityField Id = new IdentityField(META);

    //Kafka Settings
    public static final StringField BrokerList = new StringField(META, "Brokers", SFieldFlags.SMANDATORY);
    public static final StringField TagHistoryTopic = new StringField(META, "TagHistoryTopic", SFieldFlags.SMANDATORY);
    public static final BooleanField Enabled = new BooleanField(META, "Enabled").setDefault(false);
    public static final BooleanField UseStoreAndFwd = new BooleanField(META, "UseStoreAndFwd").setDefault(false);
    public static final BooleanField UseSSL = new BooleanField(META, "UseSSL").setDefault(false);

    public static final StringField AlarmsTopic = new StringField(META, "AlarmsTopic", SFieldFlags.SMANDATORY);
    public static final StringField Source = new StringField(META, "Source", SFieldFlags.SDESCRIPTIVE);
    public static final StringField DispPath = new StringField(META, "DispPath", SFieldFlags.SDESCRIPTIVE);
    public static final StringField SrcPath = new StringField(META, "SrcPath", SFieldFlags.SDESCRIPTIVE);
    public static final BooleanField AlarmsEnabled = new BooleanField(META, "AlarmsEnabled").setDefault(false);

    public static final StringField AuditTopic = new StringField(META, "AuditTopic", SFieldFlags.SMANDATORY);
    public static final BooleanField AuditEnabled = new BooleanField(META, "AuditEnabled").setDefault(false);

    // Categories for record entries, ordered by integer, titles come from KafkaSettingsRecord.properties
    static final Category Configuration = new Category("KafkaSettingsRecord.Category.Configuration", 1000).include(
            BrokerList, TagHistoryTopic, Enabled, UseStoreAndFwd, UseSSL
    );
    static final Category Alarms = new Category("KafkaSettingsRecord.Category.Alarms", 1001).include(
            AlarmsTopic, Source, DispPath, SrcPath, AlarmsEnabled
    );
    static final Category Audit = new Category("KafkaSettingsRecord.Category.Audit", 1002).include(
            AuditTopic, AuditEnabled
    );


    // record entry accessors
    public void setId(Long id) {
        setLong(Id, id);
    }

    public Long getId() {
        return getLong(Id);
    }

    public void setBrokerList(String brokers) {
        setString(BrokerList, brokers);
    }

    public String getBrokerList() {
        return getString(BrokerList);
    }

    public String getTagHistoryTopic() { return getString(TagHistoryTopic); }

    public void setTagHistoryTopic(String topic) { setString(TagHistoryTopic, topic); }

    public void setEnabled(Boolean enabled) {
        setBoolean(Enabled, enabled);
    }

    public Boolean getEnabled() {
        return getBoolean(Enabled);
    }

    public void setUseStoreAndFwd(Boolean useStoreAndFwd) {
        setBoolean(UseStoreAndFwd, useStoreAndFwd);
    }

    public Boolean getUseStoreAndfwd() {
        return getBoolean(UseStoreAndFwd);
    }

    public Boolean getUseSSL() { return getBoolean(UseSSL); }

    public void setUseSSL(Boolean useSSL) { setBoolean(UseSSL, useSSL);}

    public String getAlarmsTopic() { return getString(AlarmsTopic); }

    public void setAlarmsTopic(String topic) { setString(AlarmsTopic, topic); }

    public void setSource(String source) {
        setString(Source, source);
    }

    public String getAuditTopic() { return getString(AuditTopic); }

    public void setAuditTopic(String topic) { setString(AuditTopic, topic); }

    public void setAuditEnabled(Boolean auditEnabled) {
        setBoolean(AuditEnabled, auditEnabled);
    }

    public Boolean getAuditEnabled() {
        return getBoolean(AuditEnabled);
    }

    public String[] getSource() {
        String src = getString(Source);
        return Strings.isNullOrEmpty(src) ? null : src.split(",");
    }

    public String[] getDispPath() {
        String dispPath = getString(DispPath);
        return Strings.isNullOrEmpty(dispPath) ? null : dispPath.split(",");
    }

    public String[] getSrcPath() {
        String srcPath = getString(SrcPath);
        return Strings.isNullOrEmpty(srcPath) ? null : srcPath.split(",");
    }

    public void setAlarmsEnabled(Boolean alarmsEnabled) {
        setBoolean(AlarmsEnabled, alarmsEnabled);
    }

    public Boolean getAlarmsEnabled() {
        return getBoolean(AlarmsEnabled);
    }

    public String getSettingsRecord() {
        try {
            String json = new JSONObject()
                    .put("Brokers", getBrokerList())
                    .put("TagHistoryTopic", getTagHistoryTopic())
                    .put("Enabled", getEnabled())
                    .put("UseStoreAndFwd", getUseStoreAndfwd())
                    .put("UseSSL", getUseSSL())
                    .put("AlarmsTopic", getAlarmsTopic())
                    .put("Source", getSource())
                    .put("DispPath", getDispPath())
                    .put("SrcPath", getSrcPath())
                    .put("AlarmsEnabled", getAlarmsEnabled())
                    .put("AuditTopic", getAuditTopic())
                    .put("AuditEnabled", getAuditEnabled())
                    .toString();
            return json;
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public RecordMeta<?> getMeta() {
        return META;
    }
}