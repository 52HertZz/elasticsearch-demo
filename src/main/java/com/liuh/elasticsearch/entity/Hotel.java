package com.liuh.elasticsearch.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/**
 * @Author: liuhuan
 * @Date: 2023/8/14 9:19
 * @PackageName: com.liuh.elasticsearch.entity
 * @ClassName: Hotel
 * @Description: TODO
 * @Version 1.0
 */
@Data
@TableName(value = "hotel")
public class Hotel {

    private Long id;
    private String name;
    private String address;
    private Integer price;
    private Integer score;
    private String brand;
    private String city;
    private String starName;
    private String business;
    private String latitude;
    private String longitude;
    private String pic;

}
