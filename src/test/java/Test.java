/**
 * Copyright (C), 2015-2018
 * FileName: Test
 * Author: hello
 * Date: 2018/9/18 18:52
 * Description: 测试类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */

import com.phone.analytic.hive.EventDimensionUdf;
import com.phone.etl.UserAgentUtil;
import org.stringtemplate.v4.ST;

/**
 * 〈一句话功能简述〉 <br>
 * 〈测试类〉
 *
 * @author hello
 * @create 2018/9/18
 * @since 1.0.0
 */
public class Test {

    public static void main(String[] args){
//        Map<String, String> map = LogUtil.parserLog("114.197.87.216^A1496208168.276^Ahh^A/BCImg.gif?en=e_pv&p_url=http%3A%2F%2Fwww.mbeicai.com%2F%236d&p_ref=https%3A%2F%2Fwww.baidu.com%2Fbaidu.php%3Fwd%3Dbeifengwang%26issp%3D1%26f%3D8%26ie%3Dutf-8%26tn%3D95230157_hao_pg%26inputT%3D2412&tt=%E5%8C%97%E9%A3%8E%E7%BD%91-IT%E5%9C%A8%E7%BA%BF%E6%95%99%E8%82%B2java%E5%9F%B9%E8%AE%AD%2Casp.net%E5%9F%B9%E8%AE%AD%2Cphp%E5%9F%B9%E8%AE%AD%2Candroid%E5%9F%B9%E8%AE%AD%2CC%2FC%2B%2B%E5%9F%B9%E8%AE%AD-%E4%B8%AD%E5%9B%BDIT%E7%BD%91%E7%BB%9C%E6%95%99%E8%82%B2%E7%AC%AC%E4%B8%80%E5%93%81%E7%89%8C%E3%80%82&ver=1&pl=website&sdk=js&u_ud=D4289356-5BC9-47C4-8F7D-A16022833E7E&u_sd=FA47F1DG-2C1B-4F41-8C38-344040ABCCA1&c_time=145013766375&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36&b_rst=1280*768&u_mid=beicainet");
//
//        for (Map.Entry<String,String> entry :map.entrySet())
//            System.out.println(entry.getKey()+":"+entry.getValue()+",");
//        System.out.println(IpUtil.getRegionInfoByIp("120.197.87.216"));
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/46.0.2490.71 Safari/537.36&b_rst=1280*768"));


        String[] uas = {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Safari/64.0.3282.140 Safari/53.36 Edge/17.17134",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Safari/55.0.3282.140 Safari/55.36 Edge/15.17134",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Safari/55.0.3282.140 Safari/57.36 Edge/15.17134",
                " Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
                " Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/5.0",
                " Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/4.0",
                "Opera/9.80 (Windows NT 10.0; U; zh-cn) Presto/2.9.168 Version/11.50",
                "Opera/9.80 (Windows NT 10.0; U; zh-cn) Presto/2.9.168 Version/10.50",
                "Opera/9.80 (Windows NT 10.0; U; zh-cn) Presto/2.9.168 Version/9.50",
                "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 10.0; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)",
                "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 10.0; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)",
                "Mozilla/5.0 (compatible; MSIE 11.0; Windows NT 10.0; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/12.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/11.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201"};
//
        for (String u:uas){
            System.out.println(UserAgentUtil.parserUserAgent(u));
        }


//        System.out.println(new IDimensionImpl().getDimensionIdByObject(new PlatformDimension("windows")));
//        Integer integer = null;
//        int a = integer;
//        System.out.println(a);
//        System.out.println(new Integer(0).intValue());
        System.out.println(new EventDimensionUdf().evaluate("test","test"));

    }

}
