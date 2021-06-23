package com.choimory.hellojpascheduledbatch.batch;

import com.choimory.hellojpascheduledbatch.member.entity.Member;
import com.choimory.hellojpascheduledbatch.member.entity.Member2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class MemberJobConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private static final int CHUNK_SIZE = 3;

    @Bean
    public Job memberJob(){
        return jobBuilderFactory.get("memberJob")
                                .incrementer(new RunIdIncrementer())
                                .start(memberStep1())
                                .build();
    }

    @Bean
    public Step memberStep1(){
        return stepBuilderFactory.get("memberStep1")
                                    .<Member, Member2>chunk(CHUNK_SIZE)
                                    .reader(jpaPagingItemReader())
                                    .processor(itemProcessor())
                                    .writer(jpaItemWriter())
                                    .build();
    }

    @Bean
    public JpaPagingItemReader<Member> jpaPagingItemReader(){
        return new JpaPagingItemReaderBuilder<Member>().name("jpaPagingItemReader")
                                                        .entityManagerFactory(entityManagerFactory)
                                                        .pageSize(CHUNK_SIZE)
                                                        .queryString("SELECT m FROM Member m ORDER BY id ASC")
                                                        .build();
    }

    @Bean
    public ItemProcessor<Member, Member2> itemProcessor(){
        return member -> Member2.builder()
                                .id(member.getId())
                                .name(member.getName())
                                .build();
    }

    @Bean
    public JpaItemWriter<Member2> jpaItemWriter(){
        JpaItemWriter<Member2> jpaItemWriter = new JpaItemWriter();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
}
