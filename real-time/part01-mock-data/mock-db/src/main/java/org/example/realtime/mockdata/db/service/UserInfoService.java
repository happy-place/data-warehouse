package org.example.realtime.mockdata.db.service;

import org.example.realtime.mockdata.db.bean.UserInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 用户表 服务类
 * </p>
 *
 * @author zc
 * @since 2020-02-23
 */
public interface UserInfoService extends IService<UserInfo> {

    public void genUserInfos(Boolean ifClear);

}
