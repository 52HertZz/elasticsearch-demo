package com.liuh.elasticsearch.vo;

import com.liuh.elasticsearch.entity.Hotel;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: liuhuan
 * @Date: 2023/8/15 8:55
 * @PackageName: com.liuh.elasticsearch.vo
 * @ClassName: HotelVo
 * @Description: TODO
 * @Version 1.0
 */
@Data
@NoArgsConstructor
public class HotelVo {
    private Long id;
    private String name;
    private String address;
    private Integer price;
    private Integer score;
    private String brand;
    private String city;
    private String starName;
    private String business;
    private String location;
    private String pic;

    public HotelVo(Hotel hotel) {
        this.id = hotel.getId();
        this.name = hotel.getName();
        this.address = hotel.getAddress();
        this.price = hotel.getPrice();
        this.score = hotel.getScore();
        this.brand = hotel.getBrand();
        this.city = hotel.getCity();
        this.starName = hotel.getStarName();
        this.business = hotel.getBusiness();
        this.location = hotel.getLatitude() + "," + hotel.getLongitude();
        this.pic = hotel.getPic();
    }
}
