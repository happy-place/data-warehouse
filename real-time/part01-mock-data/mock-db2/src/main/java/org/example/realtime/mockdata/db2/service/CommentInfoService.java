package org.example.realtime.mockdata.db2.service;

import org.example.realtime.mockdata.db2.bean.CommentInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 商品评论表 服务类
 * </p>
 *
 * @author zhangchen
 * @since 2020-02-24
 */
public interface CommentInfoService extends IService<CommentInfo> {

    public  void genComments(Boolean ifClear);

}
