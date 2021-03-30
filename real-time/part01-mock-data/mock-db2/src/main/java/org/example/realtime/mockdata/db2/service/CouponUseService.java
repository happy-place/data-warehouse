package org.example.realtime.mockdata.db2.service;

import org.example.realtime.mockdata.db2.bean.CouponUse;
import org.example.realtime.mockdata.db2.bean.OrderDetailCoupon;
import org.example.realtime.mockdata.db2.bean.OrderInfo;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * <p>
 * 优惠券领用表 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2020-02-26
 */
public interface CouponUseService extends IService<CouponUse> {

    public void genCoupon(Boolean ifClear);

    public  void  usedCoupon(List<OrderInfo> orderInfoList);

    public Pair<List<CouponUse>,List<OrderDetailCoupon>> usingCoupon(List<OrderInfo> orderInfoList);

    public  void  saveCouponUseList( List<CouponUse> couponUseList);


}
