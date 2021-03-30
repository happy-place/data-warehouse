package org.example.realtime.mockdata.db2.service;

import org.example.realtime.mockdata.db2.bean.SkuInfo;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 库存单元表 服务类
 * </p>
 *
 * @author zc
 * @since 2020-02-23
 */
public interface SkuInfoService extends IService<SkuInfo> {

    public SkuInfo getSkuInfoById(List<SkuInfo> skuInfoList, Long skuId);

}
