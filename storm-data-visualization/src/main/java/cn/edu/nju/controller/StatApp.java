package cn.edu.nju.controller;

import cn.edu.nju.domain.ResultBean;
import cn.edu.nju.service.ResultBeanService;
import net.sf.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;

/**
 * Created by Thpffcj on 2018/4/10.
 */
@RestController
public class StatApp {

    @Autowired
    ResultBeanService resultBeanService;

    @RequestMapping(value = "/map", method = RequestMethod.GET)
    public ModelAndView map() {
        return new ModelAndView("map.html");
    }

    @RequestMapping(value = "/map_stat", method = RequestMethod.POST)
    @ResponseBody
    public List<ResultBean> mapStat() {
        List<ResultBean> results = resultBeanService.query();
        return results;
    }
}
