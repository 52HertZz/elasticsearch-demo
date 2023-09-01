package com.liuh.elasticsearch;

import com.liuh.elasticsearch.entity.Hotel;
import com.liuh.elasticsearch.mapper.HotelMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @Author: liuhuan
 * @Date: 2023/8/14 9:26
 * @PackageName: com.liuh.elasticsearch
 * @ClassName: MapperTests
 * @Description: TODO
 * @Version 1.0
 */
@SpringBootTest
public class MapperTests {

    @Autowired
    private HotelMapper hotelMapper;

    @Test
    void hotelMapperTest() {
        Hotel hotel = hotelMapper.selectById(1d);
        System.out.println(hotel.toString());
    }
}
