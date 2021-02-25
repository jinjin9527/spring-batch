package com.sylinx.springbatch.config;

import com.sylinx.springbatch.listener.MyChunkListener;
import com.sylinx.springbatch.listener.MyJobExecutionListener;
import com.sylinx.springbatch.listener.MyStepExecutionListener;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobConfig {

    @Bean
    public ChunkListener myChunkListener(){
        return new MyChunkListener();
    }

    @Bean
    public JobExecutionListener myJobExecutionListener(){
        return new MyJobExecutionListener();
    }

    @Bean
    public StepExecutionListener myStepExecutionListener(){
        return new MyStepExecutionListener();
    }
}
