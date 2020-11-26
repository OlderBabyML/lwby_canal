package com.lwby;

import com.lwby.canal.CanalClient;

public class BootStrap {
    public static void main(String[] args) {
        CanalClient client = new CanalClient();
        client.watch("172.17.255.184",11111,"example","lwby_novel.user_profiles,lwby_novel.user_wallet_record");
    }
}
