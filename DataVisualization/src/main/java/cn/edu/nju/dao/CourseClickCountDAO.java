package cn.edu.nju.dao;

import cn.edu.nju.domain.CourseClickCount;
import cn.edu.nju.utils.HBaseUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Thpffcj on 2018/1/15.
 * 实战课程访问数量数据访问层
 */
@Component
public class CourseClickCountDAO {

    /**
     * 根据天查询
     */
    public List<CourseClickCount> query(String day) throws Exception {

        List<CourseClickCount> list = new ArrayList<>();

        // 去HBase表中根据day获取实战课程对应的访问量
        Map<String, Long> map = HBaseUtils.getInstance().query("imooc_course_clickcount", day);

        for(Map.Entry<String, Long> entry: map.entrySet()) {
            CourseClickCount model = new CourseClickCount();
            model.setName(entry.getKey());
            model.setValue(entry.getValue());

            list.add(model);
        }

        return list;
    }

    public static void main(String[] args) throws Exception{
        CourseClickCountDAO dao = new CourseClickCountDAO();
        List<CourseClickCount> list = dao.query("20180115");
        for(CourseClickCount model : list) {
            System.out.println(model.getName() + " : " + model.getValue());
        }
    }
}