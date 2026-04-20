package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public class GetCacheThrottleRuleRequestHandler extends BaseGetBeanRequestHandler<CacheThrottleRule> {
    public GetCacheThrottleRuleRequestHandler() {
        super(CacheThrottleRule.class,
                new DefaultPublicExposureAuthenticationStrategy(DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication.USER),
                RestDomainType.CACHE_THROTTLE_RULE);
    }
}
