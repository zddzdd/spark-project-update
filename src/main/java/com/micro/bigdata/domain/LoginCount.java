package com.micro.bigdata.domain;

public class LoginCount {
    private String date;
    private String org_id;
    private String pc_code;
    private Integer user_count;

    public LoginCount(String date, String org_id, String pc_code, Integer user_count) {
        this.date = date;
        this.org_id = org_id;
        this.pc_code = pc_code;
        this.user_count = user_count;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getOrg_id() {
        return org_id;
    }

    public void setOrg_id(String org_id) {
        this.org_id = org_id;
    }

    public String getPc_code() {
        return pc_code;
    }

    public void setPc_code(String pc_code) {
        this.pc_code = pc_code;
    }

    public Integer getUser_count() {
        return user_count;
    }

    public void setUser_count(Integer user_count) {
        this.user_count = user_count;
    }
}
