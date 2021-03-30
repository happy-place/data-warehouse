package org.example.realtime.mockdata.db.service.impl;

import org.example.realtime.mockdata.db.bean.CartInfo;
import org.example.realtime.mockdata.db.bean.OrderInfo;
import org.example.realtime.mockdata.db.bean.SkuInfo;
import org.example.realtime.mockdata.db.constant.GmallConstant;
import org.example.realtime.mockdata.db.mapper.CartInfoMapper;
import org.example.realtime.mockdata.db.mapper.SkuInfoMapper;
import org.example.realtime.mockdata.db.mapper.UserInfoMapper;
import org.example.realtime.mockdata.db.service.CartInfoService;
import org.example.realtime.mockdata.db.service.SkuInfoService;
import org.example.realtime.mockdata.util.ParamUtil;
import org.example.realtime.mockdata.util.RandomNum;
import org.example.realtime.mockdata.util.RandomNumString;
import org.example.realtime.mockdata.util.RandomOptionGroup;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 * <p>
 * 购物车表 用户登录系统时更新冗余 服务实现类
 * </p>
 *
 * @author zc
 * @since 2020-02-24
 */
@Service
@Slf4j
public class CartInfoServiceImpl extends ServiceImpl<CartInfoMapper, CartInfo> implements CartInfoService {
    @Autowired
    SkuInfoService skuInfoService;

    @Autowired
    UserInfoMapper userInfoMapper;


    @Value("${mock.cart.count:100}")
    String cartCountString;

    @Value("${mock.cart.sku-maxcount-per-cart:3}")
    String skuMaxCountPerCart;

    @Value("${mock.date}")
    String mockDate;

    @Value("${mock.cart.source-type-rate}")
    String sourceTypeRate;

    public void genCartInfo(boolean ifClear) {
        Integer cartCount = ParamUtil.checkCount(cartCountString);
        Date date = ParamUtil.checkDate(mockDate);


        if (ifClear) {
            remove(new QueryWrapper<>());
        }
        List<SkuInfo> skuInfoList = skuInfoService.list(new QueryWrapper<SkuInfo>());
        Integer userTotal = userInfoMapper.selectCount(new QueryWrapper<>());

        List<CartInfo> cartInfoList = new ArrayList<>();
        HashSet<String> userIdAndSkuIdSet = new HashSet<>();
        for (int i = 0; i < cartCount; i++) {
            SkuInfo skuInfo = skuInfoList.get(RandomNum.getRandInt(0, skuInfoList.size() - 1));
            Long userId = RandomNum.getRandInt(1, userTotal) + 0L;
            boolean addSuccess = userIdAndSkuIdSet.add(userId + "_" + skuInfo.getId());

            if (addSuccess) {
                cartInfoList.add(initCartInfo(skuInfo, userId, date));
            }

        }
        log.warn("共生成购物车" + cartInfoList.size() + "条");
        saveBatch(cartInfoList, 100);
    }

    public CartInfo initCartInfo(SkuInfo skuInfo, Long userId, Date date) {
        Integer skuCount = ParamUtil.checkCount(skuMaxCountPerCart);

        Integer[] sourceTypeRateArray = ParamUtil.checkRate(this.sourceTypeRate, 4);
        RandomOptionGroup sourceTypeGroup = RandomOptionGroup.builder().add(GmallConstant.SOURCE_TYPE_QUREY, sourceTypeRateArray[0])
                .add(GmallConstant.SOURCE_TYPE_PROMOTION, sourceTypeRateArray[1])
                .add(GmallConstant.SOURCE_TYPE_AUTO_RECOMMEND, sourceTypeRateArray[2])
                .add(GmallConstant.SOURCE_TYPE_ACTIVITY, sourceTypeRateArray[3]).build();
        String sourceType = sourceTypeGroup.getRandStringValue();


        CartInfo cartInfo = new CartInfo();
        cartInfo.setCartPrice(skuInfo.getPrice());
        cartInfo.setImgUrl(skuInfo.getSkuDefaultImg());
        cartInfo.setSkuId(skuInfo.getId());
        cartInfo.setSkuName(skuInfo.getSkuName());
        cartInfo.setUserId(userId);
        cartInfo.setIsOrdered(0);
        cartInfo.setSkuNum(RandomNum.getRandInt(1, skuCount) + 0L);
        cartInfo.setCreateTime(date);
        cartInfo.setSourceType(sourceType);
        if (sourceType.equals(GmallConstant.SOURCE_TYPE_PROMOTION)) {
            cartInfo.setSourceId(RandomNum.getRandInt(10, 100) + 0L);
        } else if (sourceType.equals(GmallConstant.SOURCE_TYPE_ACTIVITY)) {
            cartInfo.setSourceId(RandomNum.getRandInt(1, 2) + 0L);
        }

        return cartInfo;
    }


}
