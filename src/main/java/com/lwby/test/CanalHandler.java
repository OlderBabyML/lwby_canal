package com.lwby.test;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;


public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    private String id="-999";
    private String username="-999";
    private String signature="-999";
    private String gender="-999";
    private String level="-999";
    private String experience="-999";
    private String nickname="-999";
    private String city="-999";
    private String hobbies="-999";
    private String career="-999";
    private String birthday="-999";
    private String last_login="-999";
    private String registration_date="-999";
    private String device_id="-999";
    private String binding_num="-999";
    private String blood_type="-999";
    private String email="-999";
    private String is_private="-999";
    private String cell_phone="-999";
    private String cell_email="-999";
    private String platform_id="-999";
    private String mainversion="-999";
    private String subversion="-999";
    private String channel="-999";
    private String user_status="-999";
    private String day="-999";

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        //下单操作
        if("user_profiles".equals(tableName)&& CanalEntry.EventType.INSERT==eventType){
            rowDateListKafka("user_profiles_test",1);
        }
        else if("user_profiles".equals(tableName)&&CanalEntry.EventType.UPDATE==eventType){
            rowDateListKafka("user_profiles_test",2);
        }
        else if("user_profiles".equals(tableName)&&CanalEntry.EventType.DELETE==eventType){
            rowDateListKafka("user_profiles_test",3);
        }
    }


    private void  rowDateListKafka(String kafkaTopic,Integer type){
        for (CanalEntry.RowData rowData : rowDataList) {
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                //JSONObject jsonObject = new JSONObject();
                String row="";
                for (CanalEntry.Column column : columnsList) {
                    try{
                        if(column.getName().equals("id")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                id=column.getValue();
                        }
                        else if(column.getName().equals("username")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                username=column.getValue();
                        }
                        else if(column.getName().equals("signature")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                signature=column.getValue();
                        }
                        else if(column.getName().equals("gender")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                gender=column.getValue();
                        }
                        else if(column.getName().equals("level")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                level=column.getValue();
                        }
                        else if(column.getName().equals("experience")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                experience=column.getValue();
                        }
                        else if(column.getName().equals("hobbies")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                hobbies=column.getValue();
                        }
                        else if(column.getName().equals("career")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                career=column.getValue();
                        }
                        else if(column.getName().equals("birthday")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                birthday=column.getValue();
                        }
                        else if(column.getName().equals("last_login")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                last_login=column.getValue();
                        }
                        else if(column.getName().equals("registration_date")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                registration_date=column.getValue();
                        }
                        else if(column.getName().equals("device_id")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                device_id=column.getValue();
                        }
                        else if(column.getName().equals("binding_num")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                binding_num=column.getValue();
                        }
                        else if(column.getName().equals("blood_type")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                blood_type=column.getValue();
                        }
                        else if(column.getName().equals("email")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                email=column.getValue();
                        }
                        else if(column.getName().equals("is_private")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                is_private=column.getValue();
                        }
                        else if(column.getName().equals("cell_phone")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                cell_phone=column.getValue();
                        }
                        else if(column.getName().equals("cell_email")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                cell_email=column.getValue();
                        }
                        else if(column.getName().equals("platform_id")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                platform_id=column.getValue();
                        }
                        else if(column.getName().equals("mainversion")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                mainversion=column.getValue();
                        }
                        else if(column.getName().equals("subversion")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                subversion=column.getValue();
                        }
                        else if(column.getName().equals("channel")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                channel=column.getValue();
                        }
                        else if(column.getName().equals("user_status")){
                            if(column.getValue()!=null&&!column.getValue().equals(""))
                                user_status=column.getValue();
                        }
                        day=registration_date.split(" ")[0];

                    }catch (Exception e){

                    }


                    //System.out.println(column.getName()+"::::"+column.getValue());
                    //jsonObject.put(column.getName(),column.getValue());
                }
                row=id+"\t"+username+"\t"+signature+"\t"+gender+"\t"+level+"\t"+experience+"\t"+nickname+"\t"+city+"\t"+hobbies+"\t"+career+"\t"+birthday
                        +"\t"+last_login+"\t"+registration_date+"\t"+device_id+"\t"+binding_num+"\t"+blood_type+"\t"+email+"\t"+is_private+"\t"
                        +cell_phone+"\t"+cell_email+"\t"+platform_id+"\t"+mainversion+"\t"+subversion+"\t"+channel+"\t"+user_status+"\t"+day+"\t"+type;
                //System.out.println(row);
                MyKafkaSender.send(kafkaTopic,row);

        }

    }
}

