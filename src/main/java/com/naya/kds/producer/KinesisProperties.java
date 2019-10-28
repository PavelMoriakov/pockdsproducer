package com.naya.kds.producer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

public class KinesisProperties implements AWSCredentialsProvider {

    @Override
    public AWSCredentials getCredentials() {
        return new AWSCredentials() {
            @Override
            public String getAWSAccessKeyId() {
                return null;
            }

            @Override
            public String getAWSSecretKey() {
                return null;
            }
        };
    }

    @Override
    public void refresh() {

    }

}
