package com.lwby.test;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public class CanalUserWalletRecord {
    private Long id=-999L;
    private Long user_id=-999L;
    private Integer coin_type=-999;
    private Double amount=-999D;
    private Integer type=-999;
    private Integer change_type=-999;
    private String description="-999";
    private Long source_id=-999L;
    private String create_time="-999";
    private String extra="-999";
    private String update_time="-999";
    private String day="-999";


    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalUserWalletRecord(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        //下单操作
        if("user_wallet_record".equals(tableName)&& CanalEntry.EventType.INSERT==eventType){
            rowDateListKafka("user_wallet_record_test",1);
        }
        else if("user_wallet_record".equals(tableName)&&CanalEntry.EventType.UPDATE==eventType){
            rowDateListKafka("user_wallet_record_test",2);
        }
        else if("user_wallet_record".equals(tableName)&&CanalEntry.EventType.DELETE==eventType){
            rowDateListKafka("user_wallet_record_test",3);
        }
    }
    private void  rowDateListKafka(String kafkaTopic,Integer operationType){
        for (CanalEntry.RowData rowData : rowDataList) {
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                //JSONObject jsonObject = new JSONObject();
                String row="";
                for (CanalEntry.Column column : columnsList) {
                    try {
                        if(column.getName().equals("id")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                id=Long.parseLong(column.getValue());
                        }
                        else if(column.getName().equals("user_id")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                user_id=Long.parseLong(column.getValue());
                        }
                        else if(column.getName().equals("coin_type")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                coin_type=Integer.parseInt(column.getValue());
                        }
                        else if(column.getName().equals("amount")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                amount=Double.parseDouble(column.getValue());
                        }
                        else if(column.getName().equals("type")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                type=Integer.parseInt(column.getValue());
                        }
                        else if(column.getName().equals("change_type")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                change_type=Integer.parseInt(column.getValue());
                        }
                        else if(column.getName().equals("description")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                description=column.getValue();
                        }
                        else if(column.getName().equals("source_id")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                source_id=Long.parseLong(column.getValue());
                        }
                        else if(column.getName().equals("create_time")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                create_time=column.getValue();
                        }
                        else if(column.getName().equals("extra")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                extra=column.getValue();
                        }
                        else if(column.getName().equals("update_time")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                update_time=column.getValue();
                        }

                        day=create_time.split(" ")[0];
                        //System.out.println(column.getName()+"::::"+column.getValue());
                        //jsonObject.put(column.getName(),column.getValue());
                    } catch (Exception e) {

                    }
                    row=id+"\t"+user_id+"\t"+coin_type+"\t"+amount+"\t"+type+"\t"+change_type+"\t"+description+"\t"+source_id+"\t"+create_time+"\t"+extra+"\t"+update_time
                            +"\t"+day+"\t"+operationType;
                    //System.out.println(row);
                    MyKafkaSender.send(kafkaTopic,row);


                }
        }

    }
}
