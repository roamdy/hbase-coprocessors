package com.ourpalm.hbase.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("all")
public class DateUtil {

    private static final Object lockObj = new Object();

    private static Map<String, ThreadLocal<SimpleDateFormat>> sdfMap = new ConcurrentHashMap<String, ThreadLocal<SimpleDateFormat>>();

    private static SimpleDateFormat getSdf(final String pattern) {
        ThreadLocal<SimpleDateFormat> tl = sdfMap.get(pattern);

        if (tl == null) {
            synchronized (lockObj) {
                tl = sdfMap.get(pattern);
                if (tl == null) {
                    tl = new ThreadLocal<SimpleDateFormat>() {
                    	protected SimpleDateFormat initialValue() {
                            return new SimpleDateFormat(pattern);
                        }
                    };
                    sdfMap.put(pattern, tl);
                }
            }
        }

        return tl.get();
    }

    public static boolean isYMDHMSValidDate(String str) {
	      boolean convertSuccess=true;
	      // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
	      SimpleDateFormat format = getSdf("yyyy-MM-dd HH:mm:ss");
	      try {
	    	  // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
	          format.setLenient(false);
	          format.parse(str);
	       } catch (ParseException e) {
	          // e.printStackTrace(); 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
	           convertSuccess=false;
	       } 
	       return convertSuccess;
	}

    public static String format(Date date, String pattern) {
        return getSdf(pattern).format(date);
    }

    public static Date parse(String dateStr, String pattern) throws ParseException {
        return getSdf(pattern).parse(dateStr);
    }
	
	public static String getDaysAgo(Date date,int num){
		Calendar now = Calendar.getInstance();
		now.setTime(date);
		now.set(Calendar.DATE,now.get(Calendar.DATE) - num);
		Date d = now.getTime();
		return format(d,"yyyy-MM-dd") + " 12:00:00";
	}
	
	
//	public static String getDaysAfter(Date date,int num){
//		
//		Calendar now =Calendar.getInstance();
//		now.setTime(date);
//		now.set(Calendar.DATE,now.get(Calendar.DATE)+num);
//		Date d = now.getTime();
//		return format(d,"yyyy-MM-dd") + " 12:00:00";
//		
//	}
	
	public static String getHoursAfter(Date date,int num){
		
		Calendar now =Calendar.getInstance();
		now.setTime(date);
		now.set(Calendar.HOUR,now.get(Calendar.HOUR)+num);
		Date d = now.getTime();
		return format(d,"yyyy-MM-dd HH:mm:ss");
		
	}
	
	public static String getHoursAgo(Date date,int num){
		
		Calendar now =Calendar.getInstance();
		now.setTime(date);
		now.set(Calendar.HOUR,now.get(Calendar.HOUR)-num);
		Date d = now.getTime();
		return format(d,"yyyy-MM-dd HH:mm:ss");
		
	}
	
	public static Date createEndDate(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		return cal.getTime();
	}

	public static Date createBeginDate(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(11, 0);
		cal.set(12, 0);
		cal.set(13, 0);
		return cal.getTime();
	}

	public static void main(String[] args) {
		String weekFirstDay =  DateUtil.getDaysAgo(new Date(),Integer.parseInt("7"));
		System.out.println(weekFirstDay);
	}

}
