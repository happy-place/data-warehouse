package org.example.realtime.mockdata.db2.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 * 
 * </p>
 *
 * @author zc
 * @since 2020-02-24
 */
@Data
public class OrderStatusLog implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private Long orderId;

    private String orderStatus;

    private Date operateTime;




    @Override
    public String toString() {
        return "OrderStatusLog{" +
        "id=" + id +
        ", orderId=" + orderId +
        ", orderStatus=" + orderStatus +
        ", operateTime=" + operateTime +
        "}";
    }
}
