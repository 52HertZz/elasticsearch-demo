package com.liuh.elasticsearch.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.liuh.elasticsearch.entity.Hotel;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

/**
 * @Author: liuhuan
 * @Date: 2023/8/14 9:25
 * @PackageName: com.liuh.elasticsearch.mapper
 * @ClassName: HotelMapper
 * @Description: TODO
 * @Version 1.0
 */
@Mapper
@Repository
public interface HotelMapper extends BaseMapper<Hotel> {
}
