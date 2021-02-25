package com.sylinx.springbatch.sample.advance.itemwriter;

import com.sylinx.springbatch.pojo.Customer;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.util.List;

/**
 * spring-batch
 * itemWriterToConsole
 */
//@Configuration
public class JobDemo15ItemWriterToConsole {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ChunkListener myChunkListener;

    class MyWriter implements ItemWriter<Customer> {
        @Override
        public void write(List<? extends Customer> list) throws Exception {
            System.out.println(list.size());
            list.forEach(s -> System.out.println(s));
        }
    }

    @Bean
    public Job jobDemo15_ItemWriter_Job1() {
        return jobBuilderFactory.get("jobDemo15_ItemWriter_Job1")
                .start(jobDemo15_ItemWriter_Step1())
                .build();
    }

    @Bean
    public Step jobDemo15_ItemWriter_Step1() {

        return stepBuilderFactory.get("jobDemo15_ItemWriter_Step1")
                .<Customer, Customer>chunk(5)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(myCsvFileItemReader())
                .writer(new MyWriter())
                .allowStartIfComplete(true).build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> myCsvFileItemReader() {
        FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("data/demo15.csv"));
        reader.setLinesToSkip(1);
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "name", "age", "createTime");
        DefaultLineMapper<Customer> mapper = new DefaultLineMapper<>();
        mapper.setFieldSetMapper(fieldSet -> {
            Customer customer = new Customer();
            customer.setId(fieldSet.readLong("id"));
            customer.setName(fieldSet.readString("name"));
            customer.setAge(fieldSet.readInt("age"));
            customer.setCreateTime(fieldSet.readDate("createTime", "yyyy/MM/dd hh:mm:ss"));
            return customer;
        });
        mapper.setLineTokenizer(tokenizer);
        mapper.afterPropertiesSet();
        reader.setLineMapper(mapper);
        return reader;
    }
}
