package org.example.realtime.mockdata.db2.service;

import org.example.realtime.mockdata.db2.bean.FavorInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 商品收藏表 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2020-02-24
 */
public interface FavorInfoService extends IService<FavorInfo> {

    public void  genFavors(Boolean ifClear);

}
