package test;

import org.apache.avro.TestAnnotation;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ExecutHello {
    public static void main(String[] args) throws Exception {
        /*String shell_path="/opt/hello_world.sh";
        Process ps = Runtime.getRuntime().exec(shell_path);
        ps.waitFor();//如果没有返回值到这里就可以结束了

        BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
        StringBuffer sb = new StringBuffer();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        System.out.println(sb.toString());*/
        mapTest();
    }

    public static void mapTest(){
        Map<String, Long> hourCountMap = new HashMap<String, Long>();
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();
        dateHourCountMap.put("date1", hourCountMap);
        hourCountMap.put("test",1L);

        for(String dhm:dateHourCountMap.keySet()){
            System.out.println(dhm+"="+dateHourCountMap.get(dhm).toString());
        }
    }
}
