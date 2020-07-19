package test;

import com.micro.bigdata.etl.util.UserAgentUtil;

public class TestUserAgentUtil {
    public static void main(String[] args) {
        String userAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36";
        UserAgentUtil.UserAgentInfo info = UserAgentUtil.analyticUserAgent(userAgent);
        System.out.println(info);
    }
}
