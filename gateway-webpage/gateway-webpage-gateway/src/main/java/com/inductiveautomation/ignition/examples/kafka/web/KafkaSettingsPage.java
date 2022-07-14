package com.inductiveautomation.ignition.examples.kafka.web;
import com.inductiveautomation.ignition.examples.kafka.GatewayHook;
import com.inductiveautomation.ignition.examples.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.model.IgnitionWebApp;
import com.inductiveautomation.ignition.gateway.web.components.RecordEditForm;
import com.inductiveautomation.ignition.gateway.web.pages.IConfigPage;
import com.inductiveautomation.ignition.gateway.web.models.LenientResourceModel;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.wicket.Application;

/**
 * Written By: Nick Robinson
 * Date: 22-Sept-2021
 * Content:  Create a page where the persistent settings can be edited
 */

public class KafkaSettingsPage extends RecordEditForm {
    public static final Pair<String, String> MENU_LOCATION =
            Pair.of(GatewayHook.CONFIG_CATEGORY.getName(), "kafka");

    public KafkaSettingsPage(final IConfigPage configPage){
        super(configPage, null, new LenientResourceModel("kafka.nav.settings.panelTitle"),
                ((IgnitionWebApp) Application.get()).getContext().getPersistenceInterface().find(KafkaSettingsRecord.META, 0L)

        );
    }

    @Override
    public Pair<String, String> getMenuLocation() {
        return MENU_LOCATION;
    }
}