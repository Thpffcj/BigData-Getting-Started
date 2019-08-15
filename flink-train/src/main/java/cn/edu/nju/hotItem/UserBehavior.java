package cn.edu.nju.hotItem;

/**
 * Created by thpffcj on 2019-08-14.
 */

/** 用户行为数据结构 **/
public class UserBehavior {

    public long userId;  // 用户 ID
    public long itemId;  // 商品 ID
    public int categoryId;  // 商品类目 ID
    public String behavior;  // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;  // 行为发生的时间戳，单位秒
}
