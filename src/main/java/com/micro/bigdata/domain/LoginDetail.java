package com.micro.bigdata.domain;

import java.sql.Date;
import java.sql.Timestamp;

public class LoginDetail {
    /**
     * val date = x._1
     *       x._2.foreach(info=>{
     *         val org_id = info._1
     *         val pc_code = info._2
     *         val userid = info._3
     *         val login_time = info._4
     */
    private String date;
    private String org_id;
    private String pc_code;
    private String userid;
    private Timestamp login_time;

    public LoginDetail(String date, String org_id, String pc_code, String userid, Timestamp login_time) {
        this.date = date;
        this.org_id = org_id;
        this.pc_code = pc_code;
        this.userid = userid;
        this.login_time = login_time;
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

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public Timestamp getLogin_time() {
        return login_time;
    }

    public void setLogin_time(Timestamp login_time) {
        this.login_time = login_time;
    }
}
