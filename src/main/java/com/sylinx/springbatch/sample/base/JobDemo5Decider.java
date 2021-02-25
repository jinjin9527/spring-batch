package com.sylinx.springbatch.sample.base;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

/**
 * spring-batch
 * decider
 */
//@Configuration
public class JobDemo5Decider {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job jobDemo5_Job1() {

        JobExecutionDecider myDecider = jobDemo5_decider_1();
        return jobBuilderFactory.get("jobDemo5_Job1")
                .start(jobDemo5_Decider_Step1())
                .next(myDecider)
                .from(myDecider).on("even").to(jobDemo5_Decider_Step2())
                .from(myDecider).on("odd").to(jobDemo5_Decider_Step3())
                .from(jobDemo5_Decider_Step3()).on("*").to(myDecider)
                .end().build();
    }

    @Bean
    public Step jobDemo5_Decider_Step1() {
        return stepBuilderFactory.get("jobDemo5_Decider_Step1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo5_Decider_Step1");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo5_Decider_Step2() {
        return stepBuilderFactory.get("jobDemo5_Decider_Step2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo5_Decider_Step2");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo5_Decider_Step3() {
        return stepBuilderFactory.get("jobDemo5_Decider_Step3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo5_Decider_Step3");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public JobExecutionDecider jobDemo5_decider_1(){
        return new JobExecutionDecider(){
            private int count;
            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
                count++;
                if(count % 2 == 0) {
                    return new FlowExecutionStatus("even");
                } else {
                    return new FlowExecutionStatus("odd");
                }
            }
        };
    }
}
