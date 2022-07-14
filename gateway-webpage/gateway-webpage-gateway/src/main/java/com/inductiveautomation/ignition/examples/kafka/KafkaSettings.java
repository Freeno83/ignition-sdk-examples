package com.inductiveautomation.ignition.examples.kafka;

import com.inductiveautomation.ignition.gateway.localdb.persistence.PersistentRecord;
import com.inductiveautomation.ignition.gateway.localdb.persistence.RecordMeta;

/**
 * Written By: Nick Robinson
 * Date: 22-Sept-2021
 * Content:  Creates an instance of KafkaSettings visible in /web/status/sys.internaldb
 */

public class KafkaSettings extends PersistentRecord{

    public static final RecordMeta<KafkaSettings> META = new RecordMeta<>(KafkaSettings.class, "kafkasettings");

    @Override
    public RecordMeta<?> getMeta() {
        return META;
    }
}
