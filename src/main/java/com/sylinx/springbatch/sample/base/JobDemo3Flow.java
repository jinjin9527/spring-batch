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

/**
 * spring-batch
 * flow to step
 */
//@Configuration
public class JobDemo3Flow {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job jobDemo3_Job1() {
        return jobBuilderFactory.get("jobDemo3_Job1")
                .start(jobDemo3_flow1())
                .next(jobDemo3_Step3())
                .end().build();
    }

    @Bean
    public Step jobDemo3_in_flow_Step1() {
        return stepBuilderFactory.get("jobDemo3_in_flow_Step1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo3_in_flow_Step1");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo3_in_flow_Step2() {
        return stepBuilderFactory.get("jobDemo3_in_flow_Step2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo3_in_flow_Step2");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo3_Step3() {
        return stepBuilderFactory.get("jobDemo3_Step3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo3_Step3");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Flow jobDemo3_flow1() {
        return new FlowBuilder<Flow>("jobDemo3_flow1")
                .start(jobDemo3_in_flow_Step1())
                .next(jobDemo3_in_flow_Step2())
                .build();
    }
}
