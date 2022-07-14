package com.inductiveautomation.ignition.examples.kafka.auditlogsf;

import com.inductiveautomation.ignition.gateway.datasource.SRConnection;
import com.inductiveautomation.ignition.gateway.history.DatasourceData;
import com.inductiveautomation.ignition.gateway.history.HistoryFlavor;

public class AuditRecordSFData implements DatasourceData {
    String query;
    String datasource;
    Object[] values;

    public AuditRecordSFData(String datasource, String query, Object... values) {
        this.datasource = datasource;
        this.query = query;
        this.values = values;
    }

    @Override
    public int getDataCount() {
        return 1;
    }

    @Override
    public HistoryFlavor getFlavor() {
        return DatasourceData.FLAVOR;
    }

    @Override
    public String getLoggerName() {
        return "Audit Log Store & Forward";
    }

    @Override
    public String getSignature() {
        return "SFQuery: " + datasource + " - " + query;
    }

    @Override
    public void storeToConnection(SRConnection conn) throws Exception {
        conn.runPrepUpdate(query, values);
    }
}
