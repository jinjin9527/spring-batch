package com.sylinx.springbatch.sample.base;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

/**
 * spring-batch
 * listener
 */
//@Configuration
public class JobDemo7Listener {

    @Autowired
    private ChunkListener myChunkListener;

    @Autowired
    private JobExecutionListener myJobExecutionListener;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    @Bean
    public Job jobDemo7_listener_Job1() {
        return jobBuilderFactory.get("jobDemo7_listener_Job1")
                .start(jobDemo7_Listener_Step1())
                .listener(myJobExecutionListener)
                .build();
    }

    @Bean
    public Step jobDemo7_Listener_Step1() {

        ItemReader<String> myItemReader = new ListItemReader<String>(Arrays.asList("1", "2", "3"));
        ItemWriter<String> myItemWriter = new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> list) throws Exception {
                list.forEach(s -> System.out.println(s));
            }
        };

        return stepBuilderFactory.get("jobDemo7_Listener_Step1")
                .<String, String>chunk(2).faultTolerant()
                .listener(myChunkListener)
                .reader(myItemReader)
                .writer(myItemWriter)
                .allowStartIfComplete(true).build();
    }

}
