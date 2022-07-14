package com.inductiveautomation.ignition.examples.kafka.auditlogsf;

import com.inductiveautomation.ignition.common.db.schema.ColumnProperty;
import com.inductiveautomation.ignition.common.model.ApplicationScope;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.examples.kafka.GatewayScriptModule;
import com.inductiveautomation.ignition.gateway.audit.AuditProfile;
import com.inductiveautomation.ignition.gateway.audit.AuditRecord;
import com.inductiveautomation.ignition.gateway.audit.DefaultAuditRecord;
import com.inductiveautomation.ignition.gateway.datasource.Datasource;
import com.inductiveautomation.ignition.gateway.datasource.SRConnection;
import com.inductiveautomation.ignition.gateway.datasource.query.DBQuery;
import com.inductiveautomation.ignition.gateway.datasource.query.SQLType;
import com.inductiveautomation.ignition.gateway.db.schema.DBTableSchema;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.util.DBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

public class AuditLogSF implements AuditProfile {
    private static final long TIME_BETWEEN_RETENTION_CLEARS = 1000 * 60 * 30; // 30 minutes
    private static final long MILLIS_PER_DAY = 1000 * 60 * 60 * 24;

    GatewayContext context;

    long datasourceId;
    Datasource ds;

    int retentionDays;

    boolean initialized = false;

    boolean autocreate = false;

    String tableName;
    String keyColumn;
    String timestampColumn;
    String actorColumn;
    String actorHostColumn;
    String actionColumn;
    String actionTargetColumn;
    String actionValueColumn;
    String statusCodeColumn;
    String originatingSystemColumn;
    String originatingContextColumn;

    String insertQuery;
    String deleteQuery;
    String q;

    Logger log = Logger.getLogger(getClass());
    GatewayScriptModule scriptModule;

    long lastRetentionClear = 0;

    public AuditLogSF(GatewayContext context, long datasourceId, int retentionDays, boolean autocreate,
                      String tableName, String keyColumn, String timestampColumn, String actorColumn, String actorHostColumn,
                      String actionColumn, String actionTargetColumn, String actionValueColumn, String statusCodeColumn,
                      String originatingSystemColumn, String originatingContextColumn,
                      GatewayScriptModule scriptModule) {
        this.autocreate = autocreate;
        this.context = context;
        this.datasourceId = datasourceId;
        this.retentionDays = retentionDays;
        this.tableName = tableName;
        this.keyColumn = keyColumn;
        this.timestampColumn = timestampColumn;
        this.actorColumn = actorColumn;
        this.actorHostColumn = actorHostColumn;
        this.actionColumn = actionColumn;
        this.actionTargetColumn = actionTargetColumn;
        this.actionValueColumn = actionValueColumn;
        this.statusCodeColumn = statusCodeColumn;
        this.originatingSystemColumn = originatingSystemColumn;
        this.originatingContextColumn = originatingContextColumn;
        this.scriptModule = scriptModule;
    }

    private synchronized void init() {
        try {
            ds = context.getDatasourceManager().getDatasource(datasourceId);
            if (ds == null) {
                throw new Exception("Datasource [" + datasourceId + "] does not exist.");
            }
        } catch (Exception e) {
            log.error("Error initializing audit log connection.", e);
        }

        if (initialized) {
            return;
        }

        try {
            if (autocreate) {
                checkTable();
            }

            q = ds.getTranslator().getColumnQuoteChar();

            StringBuilder sb = new StringBuilder();

            sb.append("INSERT INTO ").append(tableName);
            sb.append(" (");
            sb.append(q).append(timestampColumn).append(q).append(", ");
            sb.append(q).append(originatingSystemColumn).append(q).append(", ");
            sb.append(q).append(originatingContextColumn).append(q).append(", ");
            sb.append(q).append(actorColumn).append(q).append(", ");
            sb.append(q).append(actorHostColumn).append(q).append(", ");
            sb.append(q).append(actionColumn).append(q).append(", ");
            sb.append(q).append(actionTargetColumn).append(q).append(", ");
            sb.append(q).append(actionValueColumn).append(q).append(", ");
            sb.append(q).append(statusCodeColumn).append(q);
            sb.append(") VALUES (?,?,?,?,?,?,?,?,?)");

            insertQuery = sb.toString();

            sb.setLength(0);

            sb.append("DELETE FROM ").append(tableName).append(" WHERE ");
            sb.append(q).append(timestampColumn).append(q).append(" < ?");
            deleteQuery = sb.toString();

            initialized = true;
        } catch (Exception e) {
            log.error("Error initializing audit log tables.", e);
        }
    }

    protected void checkTable() {
        if (ds == null) {
            log.error("Error verifying database audit log table, datasource [" + datasourceId + "] doesn't exist.");
            return;
        }

        DBTableSchema schema = new DBTableSchema(tableName, ds.getTranslator());
        schema.addRequiredColumn(keyColumn, DataType.Int4,
                EnumSet.of(ColumnProperty.PrimaryKey, ColumnProperty.AutoIncrement));
        schema.addRequiredColumn(timestampColumn, DataType.DateTime, EnumSet.of(ColumnProperty.Indexed));
        schema.addRequiredColumn(actorColumn, DataType.String, null);
        schema.addRequiredColumn(actorHostColumn, DataType.String, null);
        schema.addRequiredColumn(actionColumn, DataType.String, null);
        schema.addRequiredColumn(actionTargetColumn, DataType.String, null);
        schema.addRequiredColumn(actionValueColumn, DataType.String, null);
        schema.addRequiredColumn(statusCodeColumn, DataType.Int4, null);
        schema.addRequiredColumn(originatingSystemColumn, DataType.String, null);
        schema.addRequiredColumn(originatingContextColumn, DataType.Int4, null);

        schemaUpdate(ds, schema);
    }

    private void schemaUpdate(Datasource ds, DBTableSchema schema) {
        SRConnection conn = null;

        try {
            conn = ds.getConnection();
            schema.verifyAndUpdate(conn);
            // Update columns with proper casing.
            keyColumn = schema.getCasedColumnName(keyColumn);
            timestampColumn = schema.getCasedColumnName(timestampColumn);
            actorColumn = schema.getCasedColumnName(actorColumn);
            actorHostColumn = schema.getCasedColumnName(actorHostColumn);
            actionColumn = schema.getCasedColumnName(actionColumn);
            actionTargetColumn = schema.getCasedColumnName(actionTargetColumn);
            actionValueColumn = schema.getCasedColumnName(actionValueColumn);
            statusCodeColumn = schema.getCasedColumnName(statusCodeColumn);
            originatingSystemColumn = schema.getCasedColumnName(originatingSystemColumn);
            originatingContextColumn = schema.getCasedColumnName(originatingContextColumn);
        } catch (Exception e) {
            log.error("Error verifying audit log table for [" + ds.getName() + "].", e);
        } finally {
            DBUtilities.close(conn, null);
        }
    }

    public void audit(AuditRecord record) {
        init();

        _audit(ds, record, retentionDays);
    }

    private void _audit(Datasource ds, AuditRecord record, int retentionDays) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Logging event [" + record + "] to [" + ds.getName() + "] with query: ["
                        + insertQuery + "]");
            }

            context.getHistoryManager()
                    .storeHistory(ds.getName(), new AuditRecordSFData(ds.getName(), insertQuery,
                            record.getTimestamp(), record.getOriginatingSystem(),
                            record.getOriginatingContext(), record.getActor(), record.getActorHost(),
                            record.getAction(),
                            record.getActionTarget(), record.getActionValue(), record.getStatusCode()));

            // Entrance point into kafka stream
            scriptModule.sendAuditData(record);

            long now = System.currentTimeMillis();
            if (now - lastRetentionClear > TIME_BETWEEN_RETENTION_CLEARS) {
                lastRetentionClear = now;

                SRConnection con = ds.getConnection();
                try {
                    int affected = con.runPrepUpdate(deleteQuery, new Date(now - (retentionDays * MILLIS_PER_DAY)));
                    if (affected > 0 && log.isDebugEnabled()) {
                        log.debug("Deleted " + affected + " old audit events from [" + ds.getName() + "]");
                    }
                } finally {
                    try {
                        con.close();
                    } catch (SQLException ignored) {
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Error auditing [" + record + "] to [" + ds.getName() + "], due to underlying exception.", ex);
        }
    }

    public List<AuditRecord> query(String actorFilter, String actionFilter, String actionTargetFilter,
                                   String actionValueFilter, Date startTime, Date endTime, String systemFilter, Integer contextFilter)
            throws Exception {
        init();

        DBQuery query = new DBQuery(q);
        query.addReturnColumn(timestampColumn, SQLType.TIMESTAMP);
        query.addReturnColumn(actorColumn, SQLType.VARCHAR);
        query.addReturnColumn(actorHostColumn, SQLType.VARCHAR);
        query.addReturnColumn(actionColumn, SQLType.VARCHAR);
        query.addReturnColumn(actionTargetColumn, SQLType.VARCHAR);
        query.addReturnColumn(actionValueColumn, SQLType.VARCHAR);
        query.addReturnColumn(statusCodeColumn, SQLType.INTEGER);
        query.addReturnColumn(originatingSystemColumn, SQLType.VARCHAR);
        query.addReturnColumn(originatingContextColumn, SQLType.INTEGER);

        query.addTable(tableName);

        query.addOrderBy(timestampColumn, true);

        if (startTime != null) {
            query.addWhereClause(query.ge(timestampColumn, startTime));
        }
        if (endTime != null) {
            query.addWhereClause(query.le(timestampColumn, endTime));
        }

        if (StringUtils.isNotBlank(systemFilter)) {
            query.addWhereClause(query.like(originatingSystemColumn, systemFilter));
        }
        if (contextFilter != null && contextFilter != 0) {
            DBQuery.OrClause or = new DBQuery.OrClause();
            if ((contextFilter & ApplicationScope.CLIENT) == ApplicationScope.CLIENT) {
                or.add(query.equal(originatingContextColumn, ApplicationScope.CLIENT));
            }
            if ((contextFilter & ApplicationScope.DESIGNER) == ApplicationScope.DESIGNER) {
                or.add(query.equal(originatingContextColumn, ApplicationScope.DESIGNER));
            }
            if ((contextFilter & ApplicationScope.GATEWAY) == ApplicationScope.GATEWAY) {
                or.add(query.equal(originatingContextColumn, ApplicationScope.GATEWAY));
            }
            query.addWhereClause(or);
        }
        if (StringUtils.isNotBlank(actorFilter)) {
            query.addWhereClause(query.like(actorColumn, actorFilter));
        }
        if (StringUtils.isNotBlank(actionFilter)) {
            query.addWhereClause(query.like(actionColumn, actionFilter));
        }
        if (StringUtils.isNotBlank(actionTargetFilter)) {
            query.addWhereClause(query.like(actionTargetColumn, actionTargetFilter));
        }
        if (StringUtils.isNotBlank(actionValueFilter)) {
            query.addWhereClause(query.like(actionValueColumn, actionValueFilter));
        }

        if (log.isDebugEnabled()) {
            log.debug("Issuing database audit query: " + query.getSql());
        }

        final List<AuditRecord> results = new ArrayList<AuditRecord>();

        try {
            _query(ds, query, results);
        } catch (Exception ex) {
        }
        return results;
    }

    private void _query(Datasource ds, DBQuery query, final List<AuditRecord> results) throws Exception {
        SRConnection con = ds.getConnection();

        query.execute(con, new DBQuery.StreamingHandler() {
            @Override
            public void onRow(int rowIndex, ResultSet rs) throws SQLException {
                Date timestamp = rs.getTimestamp(1);
                String actor = rs.getString(2);
                String actorHost = rs.getString(3);
                String action = rs.getString(4);
                String actionTarget = rs.getString(5);
                String actionValue = rs.getString(6);
                int statusCode = rs.getInt(7);
                String originatingSystem = rs.getString(8);
                int originatingContext = rs.getInt(9);

                results.add(new DefaultAuditRecord(action, actionTarget, actionValue, actor, actorHost,
                        originatingContext, originatingSystem, statusCode, timestamp));
            }

        });
    }

}
