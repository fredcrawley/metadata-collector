package com.dtsw;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

public class CustomListener extends MetaStoreEventListener {
    @Override
    public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("key: " + tableEvent.getKey(), "new value: " + tableEvent.getNewValue(), "old value: " + tableEvent.getOldValue());

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));


        super.onConfigChange(tableEvent);
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "isDeleteData: " + tableEvent.getDeleteData());

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));
        super.onDropTable(tableEvent);
    }


    @Override
    public void onAddPartition(AddPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "partitions: " + CollUtil.join(tableEvent.getPartitionIterator(),","));

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));
        super.onAddPartition(tableEvent);
    }

    @Override
    public void onDropPartition(DropPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "partitions: " + CollUtil.join(tableEvent.getPartitionIterator(),","));

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));

        super.onDropPartition(tableEvent);
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "newPartitions: " + tableEvent.getNewPartition(), "oldPartitions:" +tableEvent.getOldPartition());

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        super.onCreateDatabase(dbEvent);
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        super.onDropDatabase(dbEvent);
    }

    @Override
    public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
        logWithHeader(partSetDoneEvent.getClass().toString() + "-" + MapUtil.join(partSetDoneEvent.getPartitionName(), ",", ":"));
        super.onLoadPartitionDone(partSetDoneEvent);
    }

    @Override
    public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {
        super.onAddIndex(indexEvent);
    }

    @Override
    public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {
        super.onDropIndex(indexEvent);
    }

    @Override
    public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {
        super.onAlterIndex(indexEvent);
    }

    @Override
    public void onInsert(InsertEvent insertEvent) throws MetaException {

        ArrayList<String> DbAndtable = CollUtil.newArrayList(insertEvent.getClass().toString() + "-", " db: " + insertEvent.getDb(), "table: " + insertEvent.getTable());
        logWithHeader(CollUtil.join(DbAndtable, ","));

        logWithHeader(insertEvent.getClass().toString() + "-" + CollUtil.join(insertEvent.getFiles(), ","));

        logWithHeader(insertEvent.getClass().toString() + "-" + MapUtil.join(insertEvent.getPartitionKeyValues(), ",", ":"));
        super.onInsert(insertEvent);
    }

    @Override
    public Configuration getConf() {
        return super.getConf();
    }

    @Override
    public void setConf(Configuration config) {
        super.setConf(config);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomListener.class);
    private static final  ObjectMapper objMapper = new ObjectMapper();

    public CustomListener(Configuration config) {
        super(config);
        logWithHeader(" created ");
    }

    @Override
    public void onCreateTable(CreateTableEvent event) {
        logWithHeader("CreateTableEvent - new table:" + event.getTable().toString());
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        logWithHeader("AlterTableEvent - old table:" + event.getOldTable().toString());
        logWithHeader("AlterTableEvent - new table:" + event.getNewTable().toString());
    }

    private void logWithHeader(String str){
        LOGGER.info("[CustomListener][Thread: " + Thread.currentThread().getName()+"] | " + objToStr(str));
    }

    private String objToStr(String str){
        try {
            return objMapper.writeValueAsString(str);
        } catch (IOException e) {
            LOGGER.error("Error on conversion", e);
        }
        return null;
    }
}