package com.sylinx.springbatch.sample.advance.itemreader;

import com.sylinx.springbatch.pojo.Customer;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import java.util.HashMap;
import java.util.Map;

/**
 * spring-batch
 * itemReaderFromXML
 */
//@Configuration
public class JobDemo12ItemReaderFromXML {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ChunkListener myChunkListener;

    @Bean
    public Job jobDemo12_ItemReader_Job1() {
        return jobBuilderFactory.get("jobDemo12_ItemReader_Job1")
                .start(jobDemo12_ItemReader_Step1())
                .build();
    }

    @Bean
    public Step jobDemo12_ItemReader_Step1() {

        return stepBuilderFactory.get("jobDemo12_ItemReader_Step1")
                .<Customer, Customer>chunk(2)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(myXmlFileItemReader())
                .writer(list -> {
                    list.forEach(s -> System.out.println(s));
                })
                .allowStartIfComplete(true).build();
    }

    @Bean
    @StepScope
    public StaxEventItemReader<Customer> myXmlFileItemReader() {
        StaxEventItemReader<Customer> reader = new StaxEventItemReader<>();
        reader.setResource(new ClassPathResource("data/demo12.xml"));
        reader.setFragmentRootElementName("customer");
        XStreamMarshaller unmarshaller = new XStreamMarshaller();
        Map<String, Class> map = new HashMap<>();
        map.put("customer", Customer.class);
        unmarshaller.setAliases(map);
        reader.setUnmarshaller(unmarshaller);
        return reader;
    }
}
