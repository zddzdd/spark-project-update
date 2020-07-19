package com.micro.bigdata.domain;

import com.micro.bigdata.utils.DateUtils;

public class UserLogin {
    public UserLogin(Long login_time, String user_id) {
        this.login_time = login_time;
        this.user_id = user_id;
    }

    private Long login_time;
    private String user_id;

    public Long getLogin_time() {
        return login_time;
    }

    public void setLogin_time(Long login_time) {
        this.login_time = login_time;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    @Override
    public String toString() {
        String s = DateUtils.timeStamp2Date(login_time + "", "yyyy-MM-dd HH:MM:SS");
        return "UserLogin{" +
                "login_time=" + s +
                ", user_id='" + user_id + '\'' +
                '}';
    }
}
