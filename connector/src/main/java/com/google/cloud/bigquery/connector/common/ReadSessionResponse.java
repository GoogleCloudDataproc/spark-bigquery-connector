package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1beta1.Storage;

public class ReadSessionResponse {

    private final Storage.ReadSession readSession;
    private final TableInfo readTableInfo;

    public ReadSessionResponse(Storage.ReadSession readSession, TableInfo readTableInfo) {
        this.readSession = readSession;
        this.readTableInfo = readTableInfo;
    }

    public Storage.ReadSession getReadSession() {
        return readSession;
    }

    public TableInfo getReadTableInfo() {
        return readTableInfo;
    }
}
