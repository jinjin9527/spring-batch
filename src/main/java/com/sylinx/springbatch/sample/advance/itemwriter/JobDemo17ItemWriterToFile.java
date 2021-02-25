package com.sylinx.springbatch.sample.advance.itemwriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sylinx.springbatch.pojo.User;
import com.sylinx.springbatch.pojo.UserForWriter;
import lombok.SneakyThrows;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * spring-batch
 * itemWriterToFile
 */
@Configuration
public class JobDemo17ItemWriterToFile {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ChunkListener myChunkListener;

    @Autowired
    private JobExecutionListener myJobExecutionListener;

    @Autowired
    private StepExecutionListener myStepExecutionListener;

    @Autowired
    private DataSource dataSource;

    class MyWriter extends FlatFileItemWriter<User> {

        public MyWriter() throws Exception {
            super();
            this.setResource(new ClassPathResource("data\\output\\demo17.txt"));
//            this.setResource(new FileSystemResource("c:\\test\\demo17.csv"));
            ObjectMapper objectMapper = new ObjectMapper();
            this.setLineAggregator(new LineAggregator<User>(){
                @Override
                public String aggregate(User item) {
                    try {
                        String result = objectMapper.writeValueAsString(item);
                        return result;
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return "";
                    }
                }
            });
            this.afterPropertiesSet();
        }
    }

    @Bean
    public Job jobDemo17_ItemWriter_Job1() throws Exception {
        return jobBuilderFactory.get("jobDemo17_ItemWriter_Job1")
                .start(jobDemo17_ItemWriter_Step1())
                .listener(myJobExecutionListener)
                .build();
    }

    @Bean
    public Step jobDemo17_ItemWriter_Step1() throws Exception {

        return stepBuilderFactory.get("jobDemo17_ItemWriter_Step1")
                .<User, User>chunk(5)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(myDbItemReader())
                .writer(new MyWriter())
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
