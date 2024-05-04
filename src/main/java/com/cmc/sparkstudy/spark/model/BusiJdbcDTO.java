package com.cmc.sparkstudy.spark.model;

import java.util.Properties;

public class BusiJdbcDTO {
    private String driver;
    private String url;
    private String user;
    private String password;

    private BusiJdbcDTO() {
        // do nothing.
    }

    public static BusiJdbcDTO build(String url, String user, String password) {
        BusiJdbcDTO jdbcDTO = new BusiJdbcDTO();
        jdbcDTO.driver = "com.mysql.cj.jdbc.Driver";
        jdbcDTO.url = url;
        jdbcDTO.user = user;
        jdbcDTO.password = password;
        return jdbcDTO;
    }

    public Properties newBasicJdbcProps() {
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("driver", driver);
        jdbcProperties.put("user", user);
        jdbcProperties.put("password", password);
        return jdbcProperties;
    }

    public String getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
