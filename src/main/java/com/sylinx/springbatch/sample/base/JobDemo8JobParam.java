package com.sylinx.springbatch.sample.base;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.Map;

/**
 * spring-batch
 * jobParam
 */
//@Configuration
public class JobDemo8JobParam {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private Map<String, JobParameter> paramsMap = new HashMap<>();

    @Bean
    public Job jobDemo8_Params_Job1() {
        return jobBuilderFactory.get("jobDemo8_Params_Job1")
                .start(jobDemo8_Params_Step1())
                .build();
    }

    public StepExecutionListener myStepExecutionListener(){
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                paramsMap = stepExecution.getJobParameters().getParameters();
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                return null;
            }
        };
    }



    @Bean
    public Step jobDemo8_Params_Step1() {


        return stepBuilderFactory.get("jobDemo8_Params_Step1")
                .listener(myStepExecutionListener())
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("jobDemo8_Params_Step1" + " : " + paramsMap.get("info"));
                        return RepeatStatus.FINISHED;
                    }
                }).allowStartIfComplete(true).build();
    }

}
