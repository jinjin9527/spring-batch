package com.sylinx.springbatch.sample.advance.itemreader;

import com.sylinx.springbatch.pojo.Customer;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * spring-batch
 * itemReaderFromMultiCSVFile
 */
//@Configuration
public class JobDemo14ItemReaderExceptionAndRestart {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ChunkListener myChunkListener;

    @Value("classpath:/data/demo13*.csv")
    private Resource[] csvFiles;

    class RestartReader implements ItemStreamReader<Customer>{

        private FlatFileItemReader<Customer> customerFlatFileItemReader = new FlatFileItemReader<>();

        private Long curLine = 0L;

        private Boolean restart = false;

        private ExecutionContext executionContext;

        public RestartReader(){
            customerFlatFileItemReader.setResource(new ClassPathResource("data/demo14.csv"));
            customerFlatFileItemReader.setLinesToSkip(1);
            DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
            tokenizer.setNames("id", "name", "age", "createTime");
            DefaultLineMapper<Customer> mapper = new DefaultLineMapper<>();
            mapper.setLineTokenizer(tokenizer);
            mapper.setFieldSetMapper(fieldSet -> {
                Customer customer = new Customer();
                customer.setId(fieldSet.readLong("id"));
                customer.setName(fieldSet.readString("name"));
                customer.setAge(fieldSet.readInt("age"));
                customer.setCreateTime(fieldSet.readDate("createTime", "yyyy-MM-dd hh:mm:ss"));
                return customer;
            });

            mapper.afterPropertiesSet();
            customerFlatFileItemReader.setLineMapper(mapper);
        }

        @Override
        public Customer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
            Customer customer = null;
            this.curLine++;

            if(restart){
                customerFlatFileItemReader.setLinesToSkip(this.curLine.intValue() - 1);
                restart = false;
                System.out.println("Start reading from line : " + this.curLine);
            }

            customerFlatFileItemReader.open(this.executionContext);
            customer = customerFlatFileItemReader.read();

            return customer;
        }

        @Override
        public void open(ExecutionContext executionContext) throws ItemStreamException {
            this.executionContext = executionContext;
            if(executionContext.containsKey("curLine")) {
                this.curLine = executionContext.getLong("curLine");
                this.restart = true;
            } else {
                this.curLine = 0L;
                executionContext.put("curLine", this.curLine);
                this.curLine++;
                System.out.println("Start reading from line : " + this.curLine);
            }
        }

        @Override
        public void update(ExecutionContext executionContext) throws ItemStreamException {
            executionContext.put("curLine", this.curLine);
            System.out.println("currentLine : " + this.curLine);
        }

        @Override
        public void close() throws ItemStreamException {

        }
    }


    @Bean
    public Job jobDemo14_ItemReader_Job1() {
        return jobBuilderFactory.get("jobDemo14_ItemReader_Job1")
                .start(jobDemo14_ItemReader_Step1())
                .build();
    }

    @Bean
    public Step jobDemo14_ItemReader_Step1() {

        return stepBuilderFactory.get("jobDemo14_ItemReader_Step1")
                .<Customer, Customer>chunk(5)
                .faultTolerant()
                .listener(myChunkListener)
                .reader(new RestartReader())
                .writer(list -> {
                    list.forEach(s -> System.out.println(s));
                })
                .allowStartIfComplete(true).build();
    }
}
