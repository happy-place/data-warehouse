package org.example.realtime.mockdata.db2.service.impl;

import org.example.realtime.mockdata.db2.bean.SkuInfo;
import org.example.realtime.mockdata.db2.mapper.SkuInfoMapper;
import org.example.realtime.mockdata.db2.service.SkuInfoService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 库存单元表 服务实现类
 * </p>
 *
 * @author zc
 * @since 2020-02-23
 */
@Service
public class SkuInfoServiceImpl extends ServiceImpl<SkuInfoMapper, SkuInfo> implements SkuInfoService {


    public SkuInfo getSkuInfoById(List<SkuInfo> skuInfoList, Long skuId){
        for (SkuInfo skuInfo : skuInfoList) {
            if(skuInfo.getId().equals(skuId)){
                return skuInfo;
            }
        }
        return null;
    }

}
