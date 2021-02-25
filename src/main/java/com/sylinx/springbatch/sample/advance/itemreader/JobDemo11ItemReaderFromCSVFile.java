package com.sylinx.springbatch.sample.advance.itemreader;

import com.sylinx.springbatch.pojo.Customer;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/**
 * spring-batch
 * itemReaderFromCSVFile
 */
//@Configuration
public class JobDemo11ItemReaderFromCSVFile {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ChunkListener myChunkListener;

    @Bean
    public Job jobDemo11_ItemReader_Job1() {
        return jobBuilderFactory.get("jobDemo11_ItemReader_Job1")
                .start(jobDemo11_ItemReader_Step1())
                .build();
    }

    @Bean
    public Step jobDemo11_ItemReader_Step1() {

        return stepBuilderFactory.get("jobDemo11_ItemReader_Step1")
                .<Customer, Customer>chunk(2)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(myCsvFileItemReader())
                .writer(list -> {
                    list.forEach(s -> System.out.println(s));
                })
                .allowStartIfComplete(true).build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> myCsvFileItemReader() {
        FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("data/demo11.csv"));
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
