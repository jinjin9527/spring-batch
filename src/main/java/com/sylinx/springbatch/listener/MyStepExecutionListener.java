package com.sylinx.springbatch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class MyStepExecutionListener implements StepExecutionListener {
    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println(stepExecution.getStepName() + " : before");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println(stepExecution.getStepName() + " : after");
        return ExitStatus.COMPLETED;
    }
}
