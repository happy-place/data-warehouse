package org.example.realtime.mockdata.db.service.impl;

import org.example.realtime.mockdata.db.bean.OrderDetail;
import org.example.realtime.mockdata.db.mapper.OrderDetailMapper;
import org.example.realtime.mockdata.db.service.OrderDetailService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 订单明细表 服务实现类
 * </p>
 *
 * @author zc
 * @since 2020-02-23
 */
@Service
public class OrderDetailServiceImpl extends ServiceImpl<OrderDetailMapper, OrderDetail> implements OrderDetailService {

}
