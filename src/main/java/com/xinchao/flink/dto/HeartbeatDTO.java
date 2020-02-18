package com.xinchao.flink.dto;

public class HeartbeatDTO {
    /**
     * 时间
     */
    private String time;
    /**
     * 终端编码
     */
    private String deviceCode;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDeviceCode() {
        return deviceCode;
    }

    public void setDeviceCode(String deviceCode) {
        this.deviceCode = deviceCode;
    }

    @Override
    public String toString() {
        return "HeartbeatDTO{" +
                "time='" + time + '\'' +
                ", deviceCode='" + deviceCode + '\'' +
                '}';
    }
}
