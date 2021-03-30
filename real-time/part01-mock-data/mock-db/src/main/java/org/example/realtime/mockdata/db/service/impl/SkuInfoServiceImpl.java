package org.example.realtime.mockdata.db.service.impl;

import org.example.realtime.mockdata.db.bean.SkuInfo;
import org.example.realtime.mockdata.db.mapper.SkuInfoMapper;
import org.example.realtime.mockdata.db.service.SkuInfoService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

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

}
