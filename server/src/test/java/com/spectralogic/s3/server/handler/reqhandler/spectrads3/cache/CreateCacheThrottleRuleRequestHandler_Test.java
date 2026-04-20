package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public final class CreateCacheThrottleRuleRequestHandler_Test {
    @Test
    public void testCreateCacheThrottleRuleNoPriorityBucketNorTypeNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, ".8");
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(
                0,
                support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).getCount(),
                "Should not have created a rule."
        );
    }

    @Test
    public void testCreateCacheThrottleRuleNoMaxCacheNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.PRIORITY, BlobStoreTaskPriority.NORMAL.toString());
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(
                0,
                support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).getCount(),
                "Should not have created a rule."
        );
    }

    @Test
    public void testCreateCacheThrottleRuleMaxCacheZeroNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.PRIORITY, BlobStoreTaskPriority.NORMAL.toString())
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, "0");
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(
                0,
                support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).getCount(),
                "Should not have created a rule."
        );
    }

    @Test
    public void testCreateCacheThrottleRuleMaxCacheGreaterThanOneNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.PRIORITY, BlobStoreTaskPriority.NORMAL.toString())
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, "1.1");
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(
                0,
                support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).getCount(),
                "Should not have created a rule."
        );
    }

    @Test
    public void testCreateCacheThrottleRuleNonExistentBucketNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.BUCKET_ID, UUID.randomUUID().toString())
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, ".8");
        driver.run();
        driver.assertHttpResponseCodeEquals( 500 );

        assertEquals(
                0,
                support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).getCount(),
                "Should not have created a rule."
        );
    }

    @Test
    public void testCreateCacheThrottleRuleCollisionNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket testBucket = mockDaoDriver.createBucket(null, "a");

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.BUCKET_ID, testBucket.getId().toString())
                .addParameter(
                        CacheThrottleRule.REQUEST_TYPE, JobRequestType.GET.toString())
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, ".8");
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );

        final MockHttpRequestDriver driver2 = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.BUCKET_ID, testBucket.getId().toString())
                .addParameter(
                        CacheThrottleRule.REQUEST_TYPE, JobRequestType.GET.toString())
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, ".9");
        driver2.run();
        driver2.assertHttpResponseCodeEquals( 409 );

        assertEquals(
                1,
                support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).getCount(),
                "Should not have created a rule."
        );
    }

    @Test
    public void testCreateCacheThrottleRuleOneLimiterAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.PRIORITY, BlobStoreTaskPriority.NORMAL.toString())
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, ".8");
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );

        support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).attain(Require.nothing());
    }

    @Test
    public void testCreateCacheThrottleRuleAllLimitersAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket testBucket = mockDaoDriver.createBucket(null, "a");

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE )
                .addParameter(
                        CacheThrottleRule.PRIORITY, BlobStoreTaskPriority.NORMAL.toString())
                .addParameter(
                        CacheThrottleRule.BUCKET_ID, testBucket.getId().toString())
                .addParameter(
                        CacheThrottleRule.REQUEST_TYPE, JobRequestType.GET.toString())
                .addParameter(
                        CacheThrottleRule.MAX_CACHE_PERCENT, ".8");
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );

        support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).attain(Require.nothing());
    }
}
