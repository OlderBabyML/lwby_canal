package com.lwby.test;

public class BootStrap {
    public static void main(String[] args) {
        CanalClient client = new CanalClient();
        client.watch("172.17.255.184",11111,"example","lwby.user_profiles,lwby.user_wallet_record");
    }
}
