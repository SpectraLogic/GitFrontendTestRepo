package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ModifyCacheThrottleRuleRequestHandler_Test {

    public static CacheThrottleRule createTestPriorityCacheThrottleRule(final MockHttpRequestSupport support, final BlobStoreTaskPriority priority) {
        final CacheThrottleRule rule = BeanFactory.newBean(CacheThrottleRule.class);
        rule.setMaxCachePercent(.5).setPriority(priority).setId(UUID.randomUUID());
        support.getDatabaseSupport().getServiceManager().getCreator(CacheThrottleRule.class).create(rule);
        return rule;
    }

    @Test
    public void testModifyCacheThrottleRuleNoPriorityBucketNorTypeNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final CacheThrottleRule rule = createTestPriorityCacheThrottleRule(support, BlobStoreTaskPriority.NORMAL);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE + "/" + rule.getId())
                .addParameter(
                        CacheThrottleRule.PRIORITY, null);
        driver.run();
        driver.assertHttpResponseCodeEquals(400);

        final CacheThrottleRule updatedRule = support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).attain(Require.nothing());
        assertEquals(rule.getBucketId(), updatedRule.getBucketId(), "Rule should not have changed bucket.");
        assertEquals(rule.getPriority(), updatedRule.getPriority(), "Rule should not have changed priority.");
        assertEquals(rule.getRequestType(), updatedRule.getRequestType(), "Rule should not have changed request type.");
        assertEquals(rule.getMaxCachePercent(), updatedRule.getMaxCachePercent(), "Rule should not have changed max cache percent.");
    }

    @Test
    public void testModifyCacheThrottleRuleCollisionNotAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        createTestPriorityCacheThrottleRule(support, BlobStoreTaskPriority.NORMAL);
        final CacheThrottleRule rule2 = createTestPriorityCacheThrottleRule(support, BlobStoreTaskPriority.HIGH);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE + "/" + rule2.getId())
                .addParameter(
                        CacheThrottleRule.PRIORITY, BlobStoreTaskPriority.NORMAL.toString());
        driver.run();
        driver.assertHttpResponseCodeEquals(409);

        final CacheThrottleRule updatedRule = support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).attain(rule2.getId());
        assertEquals(rule2.getBucketId(), updatedRule.getBucketId(), "Rule should not have changed bucket.");
        assertEquals(rule2.getPriority(), updatedRule.getPriority(), "Rule should not have changed priority.");
        assertEquals(rule2.getRequestType(), updatedRule.getRequestType(), "Rule should not have changed request type.");
        assertEquals(rule2.getMaxCachePercent(), updatedRule.getMaxCachePercent(), "Rule should not have changed max cache percent.");
    }

    @Test
    public void testModifyCacheThrottleRuleUpdateAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final CacheThrottleRule rule = createTestPriorityCacheThrottleRule(support, BlobStoreTaskPriority.NORMAL);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE + "/" + rule.getId())
                .addParameter(
                        CacheThrottleRule.PRIORITY, BlobStoreTaskPriority.HIGH.toString());
        driver.run();
        driver.assertHttpResponseCodeEquals(200);

        final CacheThrottleRule updatedRule = support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).attain(Require.nothing());
        assertEquals(rule.getBucketId(), updatedRule.getBucketId(), "Rule should not have changed bucket.");
        assertEquals(BlobStoreTaskPriority.HIGH, updatedRule.getPriority(), "Rule should have updated priority.");
        assertEquals(rule.getRequestType(), updatedRule.getRequestType(), "Rule should not have changed request type.");
        assertEquals(rule.getMaxCachePercent(), updatedRule.getMaxCachePercent(), "Rule should not have changed max cache percent.");
    }
}
