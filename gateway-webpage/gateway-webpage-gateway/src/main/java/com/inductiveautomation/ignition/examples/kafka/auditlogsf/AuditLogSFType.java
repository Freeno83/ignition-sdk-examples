package com.inductiveautomation.ignition.examples.kafka.auditlogsf;

import com.inductiveautomation.ignition.examples.kafka.GatewayScriptModule;
import com.inductiveautomation.ignition.examples.kafka.records.AuditLogSFSettings;
import com.inductiveautomation.ignition.gateway.audit.AuditProfile;
import com.inductiveautomation.ignition.gateway.audit.AuditProfileRecord;
import com.inductiveautomation.ignition.gateway.audit.AuditProfileType;
import com.inductiveautomation.ignition.gateway.localdb.persistence.PersistentRecord;
import com.inductiveautomation.ignition.gateway.localdb.persistence.RecordMeta;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;

public class AuditLogSFType extends AuditProfileType {

    GatewayScriptModule scriptModule;

    public AuditLogSFType(GatewayScriptModule scriptModule) {
        super("AuditLogSF", "AuditLogSFSettings.Name", "AuditLogSFSettings.Desc");
        this.scriptModule = scriptModule;
    }

    @Override
    public RecordMeta<? extends PersistentRecord> getSettingsRecordType() {
        return AuditLogSFSettings.META;
    }

    @Override
    public AuditProfile createNewProfile(AuditProfileRecord parentSettings, GatewayContext context) throws Exception {
        AuditLogSFSettings settings = (AuditLogSFSettings) findProfileSettingsRecord(context,
                parentSettings);

        if (settings == null) {
            throw new NullPointerException("No properties found for audit profile: " + parentSettings.getName());
        }

        return new AuditLogSF(context,
                settings.getLong(AuditLogSFSettings.DatasourceId),
                settings.getInt(AuditLogSFSettings.Retention),
                settings.getBoolean(AuditLogSFSettings.AutoCreate),
                settings.getString(AuditLogSFSettings.TableName),
                settings.getString(AuditLogSFSettings.KeyColumn),
                settings.getString(AuditLogSFSettings.TimestampColumn),
                settings.getString(AuditLogSFSettings.ActorColumn),
                settings.getString(AuditLogSFSettings.ActorHostColumn),
                settings.getString(AuditLogSFSettings.ActionColumn),
                settings.getString(AuditLogSFSettings.ActionTargetColumn),
                settings.getString(AuditLogSFSettings.ActionValueColumn),
                settings.getString(AuditLogSFSettings.StatusCodeColumn),
                settings.getString(AuditLogSFSettings.OriginatingSystemColumn),
                settings.getString(AuditLogSFSettings.OriginatingContextColumn),
                scriptModule);
    }

}
