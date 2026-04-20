package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public class GetCacheThrottleRulesRequestHandler extends BaseGetBeansRequestHandler<CacheThrottleRule> {
    public GetCacheThrottleRulesRequestHandler() {
        super(CacheThrottleRule.class,
                new DefaultPublicExposureAuthenticationStrategy(DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication.USER),
                RestDomainType.CACHE_THROTTLE_RULE);

        registerOptionalBeanProperties(
                CacheThrottleRule.BUCKET_ID,
                CacheThrottleRule.MAX_CACHE_PERCENT,
                CacheThrottleRule.PRIORITY,
                CacheThrottleRule.REQUEST_TYPE,
                CacheThrottleRule.BURST_THRESHOLD);
    }
}
