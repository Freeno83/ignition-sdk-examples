package com.inductiveautomation.ignition.examples.kafka.records;

import com.inductiveautomation.ignition.gateway.audit.AuditProfileRecord;
import com.inductiveautomation.ignition.gateway.datasource.records.DatasourceRecord;
import com.inductiveautomation.ignition.gateway.localdb.persistence.*;
import simpleorm.dataset.SFieldFlags;

public class AuditLogSFSettings extends PersistentRecord {
    public final static RecordMeta<AuditLogSFSettings> META = new RecordMeta<AuditLogSFSettings>(
            AuditLogSFSettings.class, "AuditLogSFSettings");

    public static final LongField ProfileId = new LongField(META, "ProfileId", SFieldFlags.SPRIMARY_KEY);
    public static final ReferenceField<AuditProfileRecord> Profile = new ReferenceField<AuditProfileRecord>(META,
            AuditProfileRecord.META, "Profile", ProfileId);

    public static final LongField DatasourceId = new LongField(META, "DatasourceId", SFieldFlags.SMANDATORY);
    public static final ReferenceField<DatasourceRecord> Datasource = new ReferenceField<DatasourceRecord>(META,
            DatasourceRecord.META, "Datasource", DatasourceId);
    public static final IntField Retention = new IntField(META, "Retention", SFieldFlags.SMANDATORY).setDefault(90);

    public static final BooleanField AutoCreate = new BooleanField(META, "AutoCreate").setDefault(true);
    public static final StringField TableName = new StringField(META, "TableName", SFieldFlags.SMANDATORY)
            .setDefault("AUDIT_EVENTS");
    public static final StringField KeyColumn = new StringField(META, "KeyColumn", SFieldFlags.SMANDATORY)
            .setDefault("AUDIT_EVENTS_ID");
    public static final StringField TimestampColumn = new StringField(META, "TimestampColumn", SFieldFlags.SMANDATORY)
            .setDefault("EVENT_TIMESTAMP");
    public static final StringField ActorColumn = new StringField(META, "ActorColumn", SFieldFlags.SMANDATORY)
            .setDefault("ACTOR");
    public static final StringField ActorHostColumn = new StringField(META, "ActorHostColumn", SFieldFlags.SMANDATORY)
            .setDefault("ACTOR_HOST");
    public static final StringField ActionColumn = new StringField(META, "ActionColumn", SFieldFlags.SMANDATORY)
            .setDefault("ACTION");
    public static final StringField ActionTargetColumn = new StringField(META, "ActionTargetColumn",
            SFieldFlags.SMANDATORY).setDefault("ACTION_TARGET");
    public static final StringField ActionValueColumn = new StringField(META, "ActionValueColumn",
            SFieldFlags.SMANDATORY).setDefault("ACTION_VALUE");
    public static final StringField StatusCodeColumn = new StringField(META, "StatusCodeColumn", SFieldFlags.SMANDATORY)
            .setDefault("STATUS_CODE");
    public static final StringField OriginatingSystemColumn = new StringField(META, "OriginatingSystemColumn",
            SFieldFlags.SMANDATORY).setDefault("ORIGINATING_SYSTEM");
    public static final StringField OriginatingContextColumn = new StringField(META, "OriginatingContextColumn",
            SFieldFlags.SMANDATORY).setDefault("ORIGINATING_CONTEXT");

    static final Category Main = new Category("AuditLogSFSettings.Category.Main.Name", 1000).include(Datasource,
            Retention, TableName, AutoCreate);

    static final Category Columns = new Category("DatasourceAuditProfileProperties.Category.Columns.Name", 2000, true)
            .include(KeyColumn, TimestampColumn, ActorColumn, ActorHostColumn, ActionColumn, ActionTargetColumn,
                    ActionValueColumn, StatusCodeColumn, OriginatingSystemColumn, OriginatingContextColumn);

    @Override
    public RecordMeta<?> getMeta() {
        return META;
    }

    static {
        Profile.getFormMeta().setVisible(false);
        AuditProfileRecord.Retention.getFormMeta().setVisible(false);
    }
}
