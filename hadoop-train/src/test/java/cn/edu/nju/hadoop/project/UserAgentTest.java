package cn.edu.nju.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Thpffcj on 2018/1/8.
 */
public class UserAgentTest {

    @Test
    public void testReadFile() throws Exception {

        String path = "D:/Idea/code/HadoopTrain/src/resources/log.txt";

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(new File(path))));
        String line = "";

        Map<String, Integer> browserMap = new HashMap<>();

        UserAgentParser userAgentParser = new UserAgentParser();
        while (line != null) {
            line = reader.readLine();
            if (StringUtils.isNotBlank(line)) {
                String source = line.substring(getCharacterPosition(line, "\"", 5)) + 1;
                UserAgent agent = userAgentParser.parse(source);

                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String os = agent.getOs();
                String platform = agent.getPlatform();
                boolean isMobile = agent.isMobile();

                Integer browserValue = browserMap.get(browser);
                if (browserValue != null) {
                    browserMap.put(browser, browserValue + 1);
                } else {
                    browserMap.put(browser, 1);
                }

                System.out.println(browser + " , " + engine + " , " + engineVersion + " , " + os + " , "
                        + platform + " , " + isMobile);
            }
        }
        for (Map.Entry<String, Integer> entry : browserMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    /**
     * 获取指定字符串中指定标识的字符串出现的索引位置
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private int getCharacterPosition(String value, String operator, int index) {
        Matcher slashmatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while (slashmatcher.find()) {
            mIdx++;
            if (mIdx == index) {
                break;
            }
        }
        return slashmatcher.start();
    }

    @Test
    public void testUserAgentParser() {

        String source = "Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0";

        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);

        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        boolean isMobile = agent.isMobile();

        System.out.println(browser + " , " + engine + " , " + engineVersion + " , " + os + " , "
                + platform + " , " + isMobile);
    }
}
