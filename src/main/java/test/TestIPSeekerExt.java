package test;

import com.micro.bigdata.etl.util.IPSeekerExt.RegionInfo;
import com.micro.bigdata.etl.util.IPSeekerExt;


public class TestIPSeekerExt {
    public static void main(String[] args) {
        IPSeekerExt ipSeekerExt = new IPSeekerExt();
        RegionInfo regionInfo = ipSeekerExt.analyticIp("114.114.114.114");
        System.out.println(regionInfo);
        /*List <String>ips = ipSeekerExt.getAllIp();
        for(String ip:ips){
            System.out.println(ip+"---"+ipSeekerExt.analyticIp(ip));
        }*/

    }
}
