package org.example.realtime.mockdata.db.service.impl;

import org.example.realtime.mockdata.db.bean.OrderDetail;
import org.example.realtime.mockdata.db.bean.OrderInfo;
import org.example.realtime.mockdata.db.bean.OrderStatusLog;
import org.example.realtime.mockdata.db.bean.PaymentInfo;
import org.example.realtime.mockdata.db.constant.GmallConstant;
import org.example.realtime.mockdata.db.mapper.PaymentInfoMapper;
import org.example.realtime.mockdata.db.service.CouponUseService;
import org.example.realtime.mockdata.db.service.OrderInfoService;
import org.example.realtime.mockdata.db.service.OrderStatusLogService;
import org.example.realtime.mockdata.db.service.PaymentInfoService;
import org.example.realtime.mockdata.util.ParamUtil;
import org.example.realtime.mockdata.util.RanOpt;
import org.example.realtime.mockdata.util.RandomNumString;
import org.example.realtime.mockdata.util.RandomOptionGroup;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.example.realtime.mockdata.db.constant.GmallConstant.*;

/**
 * <p>
 * 支付流水表 服务实现类
 * </p>
 *
 * @author zc
 * @since 2020-02-24
 */
@Service
@Slf4j
public class PaymentInfoServiceImpl extends ServiceImpl<PaymentInfoMapper, PaymentInfo> implements PaymentInfoService {


    @Autowired
    OrderInfoService orderInfoService;

    @Autowired
    OrderStatusLogService orderStatusLogService;

    @Autowired
    CouponUseService couponUseService;


    @Value("${mock.date}")
    String mockDate;

    @Value("${mock.payment.rate:70}")
    String ifPaymentRate;

    @Value("${mock.payment.payment-type:30:60:10}")
    String paymentTypeRate;


    public void genPayments(Boolean ifClear) {
        Date date = ParamUtil.checkDate(mockDate);
        Integer ifPaymentWeight = ParamUtil.checkRatioNum(this.ifPaymentRate);
        Integer[] paymentTypeRateWeight = ParamUtil.checkRate(this.paymentTypeRate, 3);

        RandomOptionGroup<Boolean> ifPayment = new RandomOptionGroup(new RanOpt(true, ifPaymentWeight), new RanOpt(false, 100 - ifPaymentWeight));
        RandomOptionGroup<String> paymentOptionGroup = new RandomOptionGroup(new RanOpt(PAYMENT_TYPE_ALIPAY, paymentTypeRateWeight[0]), new RanOpt(PAYMENT_TYPE_WECHAT, paymentTypeRateWeight[1]), new RanOpt(PAYMENT_TYPE_UNION, paymentTypeRateWeight[2]));


        if (ifClear) {
            remove(new QueryWrapper<PaymentInfo>());
        }


        QueryWrapper<OrderInfo> orderInfoQueryWrapper = new QueryWrapper<>();
        orderInfoQueryWrapper.eq("order_status", GmallConstant.ORDER_STATUS_UNPAID);
        orderInfoQueryWrapper.orderByAsc("id");
        List<OrderInfo> orderInfoList = orderInfoService.listWithDetail(orderInfoQueryWrapper);
        List<PaymentInfo> paymentList = new ArrayList();

        if (orderInfoList.size() == 0) {
            System.out.println("没有需要支付的订单！！ ");
            return;
        }
        for (OrderInfo orderInfo : orderInfoList) {
            if (ifPayment.getRandBoolValue()) {
                PaymentInfo paymentInfo = new PaymentInfo();
                paymentInfo.setOrderId(orderInfo.getId());
                paymentInfo.setTotalAmount(orderInfo.getFinalTotalAmount());
                paymentInfo.setUserId(orderInfo.getUserId());
                paymentInfo.setOutTradeNo(orderInfo.getOutTradeNo());
                paymentInfo.setAlipayTradeNo(RandomNumString.getRandNumString(1, 9, 34, ""));
                paymentInfo.setPaymentType(paymentOptionGroup.getRandStringValue());
                paymentInfo.setSubject(orderInfo.getTradeBody());
                paymentInfo.setPaymentTime(date);
                paymentList.add(paymentInfo);

                orderInfo.setOrderStatus(GmallConstant.ORDER_STATUS_PAID);
            }
        }

        couponUseService.usedCoupon(orderInfoList);

        orderInfoService.updateOrderStatus(orderInfoList);
        log.warn("共有" + paymentList.size() + "订单完成支付");
        saveBatch(paymentList, 100);


    }
}
