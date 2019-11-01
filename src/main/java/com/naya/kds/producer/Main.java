package com.naya.kds.producer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static String streamName = "test";


    public static void main(String[] args) throws InterruptedException, JsonMappingException {

        AmazonKinesis kinesisClient = getAmazonKinesisClient("us-east-2");


            sendData(kinesisClient, streamName);

    }

    private static AmazonKinesis getAmazonKinesisClient(String regionName) {

        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("kinesis.us-east-2.amazonaws.com",
                        regionName));
        clientBuilder.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        clientBuilder.setClientConfiguration(new ClientConfiguration());

        return clientBuilder.build();
    }

    private static void sendData(AmazonKinesis kinesisClient,String streamName) throws JsonMappingException {


        Person person = new Person("John", "Smith", 20);

        Employee em = new Employee("Vasia",11,new String[]{"ASd","assasssa"});

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();

        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(AvroUtils.writeDatatoBytes(em)));

            //putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);
    }


}
