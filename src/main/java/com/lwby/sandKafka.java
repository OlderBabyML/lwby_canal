package com.lwby;

import com.lwby.kafka.MyKafkaSender;

public class sandKafka {
    public static void main(String[] args) {
        while (true){
            try {
                MyKafkaSender.send("user_profiles", "maliang123");
                //System.out.println(send);

            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }
}
