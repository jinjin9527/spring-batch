package com.sylinx.springbatch.sample.base;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * spring-batch
 * multi
 */
//@Configuration
public class JobDemo4Multi {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job jobDemo4_Job1() {
        return jobBuilderFactory.get("jobDemo4_Job1")
                .start(jobDemo4_flow1())
                .split(new SimpleAsyncTaskExecutor())
                .add(jobDemo4_flow2())
                .end().build();
    }

    @Bean
    public Step jobDemo4_Split_Step1() {
        return stepBuilderFactory.get("jobDemo4_Split_Step1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo4_Split_Step1");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo4_Split_Step2() {
        return stepBuilderFactory.get("jobDemo4_Split_Step2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo4_Split_Step2");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo4_Split_Step3() {
        return stepBuilderFactory.get("jobDemo4_Split_Step3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo4_Split_Step3");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Flow jobDemo4_flow1() {
        return new FlowBuilder<Flow>("jobDemo4_flow1")
                .start(jobDemo4_Split_Step1())
                .build();
    }

    @Bean
    public Flow jobDemo4_flow2() {
        return new FlowBuilder<Flow>("jobDemo4_flow2")
                .start(jobDemo4_Split_Step2())
                .next(jobDemo4_Split_Step3())
                .build();
    }
}
