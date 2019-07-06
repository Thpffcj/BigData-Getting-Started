package cn.edu.nju.course05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by thpffcj on 2019-07-05.
 */
public class JavaCustomNonParallelSourceFunction implements SourceFunction<Long> {

    boolean isRunning = true;
    Long count = 1L;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
