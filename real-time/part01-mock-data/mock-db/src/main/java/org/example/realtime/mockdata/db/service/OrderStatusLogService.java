package org.example.realtime.mockdata.db.service;

import org.example.realtime.mockdata.db.bean.OrderInfo;
import org.example.realtime.mockdata.db.bean.OrderStatusLog;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 服务类
 * </p>
 *
 * @author zc
 * @since 2020-02-24
 */
public interface OrderStatusLogService extends IService<OrderStatusLog> {
    public void genOrderStatusLog(List<OrderInfo> orderInfoList);

}
