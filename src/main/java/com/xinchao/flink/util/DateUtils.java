package com.xinchao.flink.util;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    public static long stringDateTimeToLong(String dateTime, String pattern) {
        if (StringUtils.isBlank(dateTime) || StringUtils.isBlank(pattern)) {
            throw new NullPointerException();
        }
        LocalDate now = LocalDate.now();
        LocalTime localTime = LocalTime.parse(dateTime, DateTimeFormatter.ofPattern(pattern));
        LocalDateTime localDateTime = LocalDateTime.of(now, localTime);
        return localDateTime.atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

}
