package com.sylinx.springbatch.sample.base;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * spring-batch
 * decider
 */
//@Configuration
public class JobDemo6JobNest {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    @Bean
    public Job jobDemo6_child_Job1() {
        return jobBuilderFactory.get("jobDemo6_child_Job1")
                .start(jobDemo6_JobNest_Step1())
                .build();
    }

    @Bean
    public Job jobDemo6_child_Job2() {
        return jobBuilderFactory.get("jobDemo6_child_Job2")
                .start(jobDemo6_JobNest_Step2())
                .next(jobDemo6_JobNest_Step3())
                .build();
    }

    @Bean
    public Job jobDemo6_parent_Job(JobRepository repository, PlatformTransactionManager transactionManager) {
        Step childJob1 = new JobStepBuilder(new StepBuilder("jobDemo6_child_Job1"))
                .job(jobDemo6_child_Job1())
                .launcher(jobLauncher)
                .allowStartIfComplete(true)
                .repository(repository)
                .transactionManager(transactionManager)
                .build();
        Step childJob2 = new JobStepBuilder(new StepBuilder("jobDemo6_child_Job2"))
                .job(jobDemo6_child_Job2())
                .launcher(jobLauncher)
                .allowStartIfComplete(true)
                .repository(repository)
                .transactionManager(transactionManager)
                .build();

        return jobBuilderFactory.get("jobDemo6_parent_Job")
                .start(childJob2)
                .next(childJob1)
                .build();
    }

    @Bean
    public Step jobDemo6_JobNest_Step1() {
        return stepBuilderFactory.get("jobDemo6_JobNest_Step1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo6_JobNest_Step1");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo6_JobNest_Step2() {
        return stepBuilderFactory.get("jobDemo6_JobNest_Step2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo6_JobNest_Step2");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }

    @Bean
    public Step jobDemo6_JobNest_Step3() {
        return stepBuilderFactory.get("jobDemo6_JobNest_Step3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("jobDemo6_JobNest_Step3");
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(true).build();
    }
}
