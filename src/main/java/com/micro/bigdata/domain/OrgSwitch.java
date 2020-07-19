package com.micro.bigdata.domain;



import java.sql.Timestamp;

public class OrgSwitch {
    private String user_id;
    private String org_id;
    private Timestamp switch_time;

    public OrgSwitch(String user_id, String org_id, Timestamp switch_time) {
        this.user_id = user_id;
        this.org_id = org_id;
        this.switch_time = switch_time;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getOrg_id() {
        return org_id;
    }

    public void setOrg_id(String org_id) {
        this.org_id = org_id;
    }

    public Timestamp getSwitch_time() {
        return switch_time;
    }

    public void setSwitch_time(Timestamp switch_time) {
        this.switch_time = switch_time;
    }
}
