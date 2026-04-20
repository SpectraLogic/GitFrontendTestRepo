package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;

public class DeleteCacheThrottleRuleRequestHandler_Test {
    @Test
    public void testDeleteCacheThrottleRuleAllowed() {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final CacheThrottleRule rule = ModifyCacheThrottleRuleRequestHandler_Test.createTestPriorityCacheThrottleRule(support, BlobStoreTaskPriority.NORMAL);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.CACHE_THROTTLE_RULE + "/" + rule.getId());
        driver.run();
        driver.assertHttpResponseCodeEquals(204);

        final CacheThrottleRule updatedRule = support.getDatabaseSupport().getServiceManager().getRetriever(CacheThrottleRule.class).retrieve(rule.getId());
        assertNull(updatedRule, "Rule should have been deleted");
    }
}
