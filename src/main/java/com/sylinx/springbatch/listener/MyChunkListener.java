package com.sylinx.springbatch.listener;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

public class MyChunkListener implements ChunkListener{

    private int count = 1;

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        System.out.println(chunkContext.getStepContext().getStepName() + " : "+  count + " : before");
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        System.out.println(chunkContext.getStepContext().getStepName() + " : "+  count + " : after");
        count++;
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        System.out.println(chunkContext.getStepContext().getStepName() + " : "+  count + " : error");
    }
}
