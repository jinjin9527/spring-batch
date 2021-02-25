package com.sylinx.springbatch.sample.advance.itemreader;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

/**
 * spring-batch
 * itemReaderHelloWorld
 */
//@Configuration
public class JobDemo9ItemReaderHelloWorld {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ChunkListener myChunkListener;

    @Bean
    public Job jobDemo9_ItemReader_Job1() {
        return jobBuilderFactory.get("jobDemo9_ItemReader_Job1")
                .start(jobDemo9_ItemReader_Step1())
                .build();
    }

    @Bean
    public Step jobDemo9_ItemReader_Step1() {

        // chunksize -> 入力値の取得回数/実行  下記は2回取得後ごとに、実行する
        return stepBuilderFactory.get("jobDemo9_ItemReader_Step1")
                .<String, String>chunk(2)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(myItemReader())
                .writer(list -> {
                    list.forEach(s -> System.out.println(s));
                })
                .allowStartIfComplete(true).build();
    }

    @Bean
    public ItemReader<String> myItemReader() {
        return new ItemReader<String>() {

            Iterator<String> iterator = Arrays.asList("1", "2", "3", "4", "5").iterator();

            @Override
            public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {

                if(this.iterator.hasNext()){
                    return this.iterator.next();
                } else {
                    return null;
                }
            }
        };
    }
}
