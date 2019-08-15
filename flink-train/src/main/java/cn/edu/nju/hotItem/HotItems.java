package cn.edu.nju.hotItem;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by thpffcj on 2019-08-14.
 * 数据集获取：
 * curl https://raw.githubusercontent.com/wuchong/my-flink-project/master/src/main/resources/UserBehavior.csv > UserBehavior.csv
 */
public class HotItems {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 为了打印到控制台的结果不乱序，我们配置全局的并发为 1，这里改变并发对结果正确性没有影响
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // UserBehavior.csv 的本地路径
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));

        // 抽取UserBehavior的TypeInformation，是一个PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于Java反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fileOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fileOrder);

        // 下一步我们用PojoCsvInputFormat创建输入源
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);

        // 这样我们就得到了一个带有时间标记的数据流了，后面就能做一些窗口的操作
        DataStream<UserBehavior> timeData = dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                // 原始数据单位秒，将其转成毫秒
                return userBehavior.timestamp * 1000;
            }
        });

        // 过滤出点击事件
        DataStream<UserBehavior> pvData = timeData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("pv");
            }
        });

        // 由于要每隔5分钟统计最近一小时每个商品的点击量，所以窗口大小是一小时，每隔5分钟滑动一次
        DataStream<ItemViewCount> windowedData = pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());


        DataStream<String> topItems = windowedData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));  // 求点击量前3名的商品

        topItems.print();

        env.execute("HotItems");
    }

    /**
     * COUNT 统计的聚合函数实现，每出现一条记录加一
     * 它能使用 AggregateFunction 提前聚合掉数据，减少 state 的存储压力
     */
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key,  // 窗口的主键，即itemId
                          TimeWindow window,  // 窗口
                          Iterable<Long> input,  // 聚合函数的结果，即count值
                          Collector<ItemViewCount> out  // 输出类型为ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = input.iterator().next();
            out.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /**
     * 商品点击量(窗口操作的输出类型)
     */
    public static class ItemViewCount {

        public long itemId; // 商品 ID
        public long windowEnd; // 窗口结束时间戳
        public long viewCount; // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

    /**
     * 求某个窗口中前N名的热门点击商品，key为窗口时间戳，输出为TopN的结果字符串
     */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        // 用户存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发TopN计算
        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<ItemViewCount>(
                    "itemState-state",
                    ItemViewCount.class
            );

            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(ItemViewCount input, Context context, Collector<String> collector) throws Exception {
            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册windowEnd+1的EventTime Timer，当触发时，说明收集了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            // 将排名信息格式化成String，便于打印
            StringBuilder result = new StringBuilder();
            result.append("========================\n");
            result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < topSize; i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1: 商品ID=12224 浏览量=2413
                result.append("No").append(i).append("：")
                        .append(" 商品ID=").append(currentItem.itemId)
                        .append(" 浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("========================\n");

            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);

            out.collect(result.toString());
        }
    }
}


