package com.amazonaws.services.glue.catalog;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory for creating AWS Glue clients from Hadoop Configuration.
 * Reads the region from {@code glue.catalog.region} (default: us-east-1)
 * and uses the default AWS credential provider chain.
 */
public class GlueClientFactory {

    static final String GLUE_CATALOG_REGION = "glue.catalog.region";
    static final String DEFAULT_REGION = "us-east-1";

    public static AWSGlue createClient(Configuration config) {
        String region = config.get(GLUE_CATALOG_REGION, DEFAULT_REGION);
        return AWSGlueClientBuilder.standard()
                .withRegion(region)
                .build();
    }
}
