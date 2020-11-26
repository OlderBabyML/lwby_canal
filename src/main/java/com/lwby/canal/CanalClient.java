package com.lwby.canal;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.lwby.table.CanalUserProfiles;
import com.lwby.table.CanalUserWalletRecord;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {


    public void watch(String hostname,int port,String destination ,String tables) {
        //连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination, "", "");
        while (true){
            canalConnector.connect();
            canalConnector.subscribe(tables);
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();

            if(size==0){
                //System.out.println("没有数据！！休息5秒");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if(  entry.getEntryType()== CanalEntry.EntryType.ROWDATA  ){

                        CanalEntry.RowChange rowChange=null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        String tableName = entry.getHeader().getTableName();// 表名
                        if ("user_profiles".equals(tableName)){
                            CanalEntry.EventType eventType = rowChange.getEventType();//insert update delete？
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集  数据
                            CanalUserProfiles canalUserProfiles = new CanalUserProfiles(eventType, tableName, rowDatasList);
                            canalUserProfiles.handle() ;
                        }else if ("user_wallet_record".equals(tableName)){
                            CanalEntry.EventType eventType = rowChange.getEventType();//insert update delete？
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集  数据
                            CanalUserWalletRecord canalHandler = new CanalUserWalletRecord(eventType, tableName, rowDatasList);
                            canalHandler.handle() ;
                        }


                    }
                }
            }
        }
    }

}

