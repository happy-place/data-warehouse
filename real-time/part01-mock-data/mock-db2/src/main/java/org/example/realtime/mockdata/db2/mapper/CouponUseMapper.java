package org.example.realtime.mockdata.db2.mapper;

import org.apache.ibatis.annotations.*;
import org.example.realtime.mockdata.db2.bean.CouponUse;
import org.example.realtime.mockdata.db2.constant.GmallConstant;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;

import java.util.List;

/**
 * <p>
 * 优惠券领用表 Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2020-02-26
 */
public interface CouponUseMapper extends BaseMapper<CouponUse> {

    @Select("select * from coupon_use where coupon_status='"+ GmallConstant.COUPON_STATUS_UNUSED +"'")
    @Results(id="couponMap", value = {
            @Result(id=true,column = "id" ,property = "id"),
            @Result(  property= "couponId" , column= "coupon_id"),
            @Result(  property= "orderId" , column= "order_id"),
            @Result(  property= "couponStatus" , column= "coupon_status"),
            @Result(  property= "getTime" , column= "get_time"),
            @Result(  property= "usingTime" , column= "expire_time"),
            @Result( property = "usedTime" , column= "userd_time"),
            @Result(  property= "expireTime" , column= "expire_time" ),
            @Result(  property= "createTime" , column= "create_time" ),
            @Result( property = "couponInfo" , column= "coupon_id" ,one = @One(select = "org.example.realtime.mockdata.db2.mapper.CouponInfoMapper.selectById") ),
            @Result( property = "couponRangeList" , column= "coupon_id" ,many = @Many(select = "org.example.realtime.mockdata.db2.mapper.CouponRangeMapper.selectById"))
            })
    public List<CouponUse> selectUnusedCouponUseListWithInfo() ;


}
