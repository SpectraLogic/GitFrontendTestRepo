package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public class GetCacheThrottleRuleRequestHandler_Test {
    @Test
    public void testGetCacheThrottleRuleAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final CacheThrottleRule rule1 = ModifyCacheThrottleRuleRequestHandler_Test.createTestPriorityCacheThrottleRule(support, BlobStoreTaskPriority.HIGH);
        final CacheThrottleRule rule2 = ModifyCacheThrottleRuleRequestHandler_Test.createTestPriorityCacheThrottleRule(support, BlobStoreTaskPriority.NORMAL);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE + "/" + rule1.getId());
        driver.run();
        driver.assertHttpResponseCodeEquals(200);
        driver.assertResponseToClientContains(rule1.getId().toString());
        driver.assertResponseToClientDoesNotContain(rule2.getId().toString());
    }
}
