import org.apache.log4j.Logger;

/**
 * Created by Thpffcj on 2018/1/14.
 * 模拟日志产生
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws Exception{

        int index = 0;
        while(true) {
            Thread.sleep(1000);
            logger.info("value : " + index++);
        }
    }
}
