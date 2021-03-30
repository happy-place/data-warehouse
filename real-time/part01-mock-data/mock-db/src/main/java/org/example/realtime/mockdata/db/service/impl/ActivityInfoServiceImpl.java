package org.example.realtime.mockdata.db.service.impl;

import org.example.realtime.mockdata.db.bean.*;
import org.example.realtime.mockdata.db.mapper.ActivityInfoMapper;
import org.example.realtime.mockdata.db.service.ActivityInfoService;
import org.example.realtime.mockdata.db.service.ActivitySkuService;
import org.example.realtime.mockdata.util.RandomNum;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 活动表 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2020-02-25
 */
@Service
public class ActivityInfoServiceImpl extends ServiceImpl<ActivityInfoMapper, ActivityInfo> implements ActivityInfoService {


}
