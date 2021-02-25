package com.sylinx.springbatch.sample.advance.itemwriter;

import com.sylinx.springbatch.pojo.UserForWriter;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * spring-batch
 * itemWriterToDB
 */
//@Configuration
public class JobDemo16ItemWriterToDB {

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

    class MyWriter extends JdbcBatchItemWriter<UserForWriter> {

        public MyWriter(DataSource dataSource){
            super();
            super.setDataSource(dataSource);
            super.setSql("insert into USER_FOR_WRITER(id, username, password, age) values(:id, :username, :password, :age);");
            super.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<UserForWriter>());
            super.afterPropertiesSet();
        }
    }

    @Bean
    public Job jobDemo16_ItemWriter_Job1() {
        return jobBuilderFactory.get("jobDemo16_ItemWriter_Job1")
                .start(jobDemo16_ItemWriter_Step1())
                .next(jobDemo16_ItemWriter_Step2())
                .listener(myJobExecutionListener)
                .build();
    }
    @Bean
    public Step jobDemo16_ItemWriter_Step1(){
        return stepBuilderFactory.get("jobDemo16_ItemWriter_Step1")
            .tasklet(new MyTasklet(dataSource)).allowStartIfComplete(true).listener(myStepExecutionListener).build();
    }

    class MyTasklet implements Tasklet {

        private JdbcTemplate jdbcTemplate;
        public MyTasklet(DataSource dataSource){
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }

        @Override
        public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
            this.jdbcTemplate.execute("truncate TABLE USER_FOR_WRITER;");
            return RepeatStatus.FINISHED;
        }
    }

    @Bean
    public Step jobDemo16_ItemWriter_Step2() {

        return stepBuilderFactory.get("jobDemo16_ItemWriter_Step2")
                .<UserForWriter, UserForWriter>chunk(5)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(myCsvFileItemReader())
                .writer(new MyWriter(dataSource))
                .allowStartIfComplete(true).build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<UserForWriter> myCsvFileItemReader() {
        FlatFileItemReader<UserForWriter> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("data/demo16.csv"));
        reader.setLinesToSkip(1);
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "username", "password", "age");
        DefaultLineMapper<UserForWriter> mapper = new DefaultLineMapper<>();
        mapper.setFieldSetMapper(fieldSet -> {
            UserForWriter userForWriter = new UserForWriter();
            userForWriter.setId(fieldSet.readLong("id"));
            userForWriter.setUsername(fieldSet.readString("username"));
            userForWriter.setPassword(fieldSet.readString("password"));
            userForWriter.setAge(fieldSet.readInt("age"));
            return userForWriter;
        });
        mapper.setLineTokenizer(tokenizer);
        mapper.afterPropertiesSet();
        reader.setLineMapper(mapper);
        return reader;
    }
}
