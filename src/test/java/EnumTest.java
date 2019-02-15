/**
 * Copyright (C), 2015-2018
 * FileName: EnumTest
 * Author: imyubao
 * Date: 2018/9/23 20:19
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */

import com.phone.common.KpiType;

import java.util.HashMap;
import java.util.Map;

/**
 * 功能简述: <br>
 *
 * @classname EnumTest
 * @author imyubao
 * @create 2018/9/23
 * @since 1.0
 */
public class EnumTest {
    public static void main(String[] args){

        KpiType k1 = KpiType.BROWSER_NEW_USER;
        KpiType k2 = KpiType.NEW_MEMBER;
//
//        Map<KpiType,Integer> map = new HashMap<KpiType,Integer>();
//        map.put(k1,1);
//        map.put(k2,2);
        System.out.println(k1.kpiName.compareTo(k2.kpiName));
    }

}
