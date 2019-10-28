package com.naya.kds.producer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) {

        AmazonKinesis kinesisClient = getAmazonKinesisClient("");

        sendData(kinesisClient,"JavaTest");
    }

    private static AmazonKinesis getAmazonKinesisClient(String regionName) {

        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

        clientBuilder.setRegion(regionName);
        clientBuilder.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        //clientBuilder.withCredentials(new KinesisProperties());
        clientBuilder.setClientConfiguration(new ClientConfiguration());

        return clientBuilder.build();
    }

    private static void sendData(AmazonKinesis kinesisClient,String streamName) {

        ObjectMapper mapper = new ObjectMapper();

        Person person = new Person("John", "Smith", 20);

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
           /* try {
                putRecordsRequestEntry.setData(ByteBuffer.wrap(mapper.writeValueAsBytes(person)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }*/
            putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);
    }
}
