package org.example.realtime.mockdata.db2.bean;

import com.baomidou.mybatisplus.annotation.TableField;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class OrderDetailCoupon {


    private  Long id;

    private Long orderId;

    private Long orderDetailId;

    private Long couponId;

    private Long skuId;

    private Date createTime;

    @TableField(exist = false)
    private OrderDetail orderDetail;
    @TableField(exist = false)
    private OrderInfo orderInfo;
    @TableField(exist = false)
    private CouponInfo couponInfo;

    @TableField(exist = false)
    private CouponUse couponUse;

}
