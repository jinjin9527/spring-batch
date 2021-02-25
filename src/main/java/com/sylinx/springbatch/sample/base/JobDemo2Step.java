package com.sylinx.springbatch.sample.base;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

/**
 * spring-batch
 * step to step
 */
//@Configuration
public class JobDemo2Step {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job jobDemo2_Job1() {
        return jobBuilderFactory.get("jobDemo2_Job1")
                .start(jobDemo2_Step1())
                .next(jobDemo2_Step2())
                .next(jobDemo3_Step3())
                .build();
    }

    @Bean
    public Job jobDemo2_Job2() {
        return jobBuilderFactory.get("jobDemo2_Job2")
                .start(jobDemo2_Step1())
                .on("COMPLETED")
                .to(jobDemo2_Step2()).from(jobDemo2_Step2())
                .on("COMPLETED")
                // fail() stopAndRestart()
                .to(jobDemo3_Step3()).from(jobDemo3_Step3())
                .end().build();
    }


    @Bean
    public Step jobDemo2_Step1(){
        return stepBuilderFactory.get("jobDemo2_Step1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo2_Step1");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }
    @Bean
    public Step jobDemo2_Step2(){
        return stepBuilderFactory.get("jobDemo2_Step2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo2_Step2");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step jobDemo3_Step3(){
        return stepBuilderFactory.get("jobDemo3_Step3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo3_Step3");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }
}
