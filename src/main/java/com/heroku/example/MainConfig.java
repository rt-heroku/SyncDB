package com.heroku.example;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.URISyntaxException;

@Configuration
@ComponentScan(basePackages = "com.heroku.example")
public class MainConfig {

    @Bean
    public BasicDataSource dataSource() throws URISyntaxException {
        String dbUrl = System.getenv("JDBC_DATABASE_URL");
        BasicDataSource basicDataSource = new BasicDataSource();

        if (dbUrl == null || dbUrl.equals("")){

            URI dbUri = new URI(System.getenv("DATABASE_URL"));

            String username = dbUri.getUserInfo().split(":")[0];
            String password = dbUri.getUserInfo().split(":")[1];
            dbUrl = "jdbc:postgresql://" + dbUri.getHost() + dbUri.getPath() + "?sslmode=require";

            basicDataSource.setUsername(username);
            basicDataSource.setPassword(password);

        }
        else 
            System.out.println("using JDBC -- dbUrl = [" + dbUrl + "]");

        basicDataSource.setUrl(dbUrl);

        return basicDataSource;
    }

}
