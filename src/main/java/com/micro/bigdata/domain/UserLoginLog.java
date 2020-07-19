package com.micro.bigdata.domain;

import java.sql.Timestamp;

public class UserLoginLog {
    private String user_id;
    private String org_id;
    private String platform_code;
    private String client_type;
    private String login_type;
    private String app_version;
    private String phone;
    private String hw;
    private String nw;
    private String login_host;
    private Timestamp login_time;

    public UserLoginLog(String user_id, String org_id, String platform_code, String client_type, String login_type, String app_version, String phone, String hw, String nw, String login_host, Timestamp login_time) {
        this.user_id = user_id;
        this.org_id = org_id;
        this.platform_code = platform_code;
        this.client_type = client_type;
        this.login_type = login_type;
        this.app_version = app_version;
        this.phone = phone;
        this.hw = hw;
        this.nw = nw;
        this.login_host = login_host;
        this.login_time = login_time;
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

    public String getPlatform_code() {
        return platform_code;
    }

    public void setPlatform_code(String platform_code) {
        this.platform_code = platform_code;
    }

    public String getClient_type() {
        return client_type;
    }

    public void setClient_type(String client_type) {
        this.client_type = client_type;
    }

    public String getLogin_type() {
        return login_type;
    }

    public void setLogin_type(String login_type) {
        this.login_type = login_type;
    }

    public String getApp_version() {
        return app_version;
    }

    public void setApp_version(String app_version) {
        this.app_version = app_version;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getHw() {
        return hw;
    }

    public void setHw(String hw) {
        this.hw = hw;
    }

    public String getNw() {
        return nw;
    }

    public void setNw(String nw) {
        this.nw = nw;
    }

    public String getLogin_host() {
        return login_host;
    }

    public void setLogin_host(String login_host) {
        this.login_host = login_host;
    }

    public Timestamp getLogin_time() {
        return login_time;
    }

    public void setLogin_time(Timestamp login_time) {
        this.login_time = login_time;
    }
}
