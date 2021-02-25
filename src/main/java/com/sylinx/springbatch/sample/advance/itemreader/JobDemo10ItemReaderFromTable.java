package com.sylinx.springbatch.sample.advance.itemreader;

import com.sylinx.springbatch.pojo.User;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * spring-batch
 * itemReaderFromTable
 */
//@Configuration
public class JobDemo10ItemReaderFromTable {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ChunkListener myChunkListener;

    @Autowired
    private DataSource dataSource;

    @Bean
    public Job jobDemo10_ItemReader_Job1() {
        return jobBuilderFactory.get("jobDemo10_ItemReader_Job1")
                .start(jobDemo10_ItemReader_Step1())
                .build();
    }

    @Bean
    public Step jobDemo10_ItemReader_Step1() {

        return stepBuilderFactory.get("jobDemo10_ItemReader_Step1")
                .<User, User>chunk(2)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(myDbItemReader())
                .writer(list -> {
                    list.forEach(s -> System.out.println(s));
                })
                .allowStartIfComplete(true).build();
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<User> myDbItemReader() {
        JdbcPagingItemReader<User> reader = new JdbcPagingItemReader<User>();

        reader.setDataSource(dataSource);
        reader.setFetchSize(2);
        reader.setRowMapper(new RowMapper<User>() {
            @Override
            public User mapRow(ResultSet resultSet, int i) throws SQLException {
                User user = new User();
                user.setId(resultSet.getInt(1));
                user.setUsername(resultSet.getString(2));
                user.setPassword(resultSet.getString(3));
                user.setAge(resultSet.getInt(4));
                return user;
            }
        });
        MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();
        provider.setSelectClause("id, username, password, age");
        provider.setFromClause("from USER");
        HashMap<String, Order> sortMap = new HashMap<>(1);
        sortMap.put("id", Order.DESCENDING);
        provider.setSortKeys(sortMap);
        reader.setQueryProvider(provider);
        return reader;
    }

}
