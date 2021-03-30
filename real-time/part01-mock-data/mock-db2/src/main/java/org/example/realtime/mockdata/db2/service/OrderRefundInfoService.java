package org.example.realtime.mockdata.db2.service;

import org.example.realtime.mockdata.db2.bean.OrderRefundInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 退单表 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2020-02-25
 */
public interface OrderRefundInfoService extends IService<OrderRefundInfo> {

    public void  genRefundsOrFinish(Boolean ifClear);
}
