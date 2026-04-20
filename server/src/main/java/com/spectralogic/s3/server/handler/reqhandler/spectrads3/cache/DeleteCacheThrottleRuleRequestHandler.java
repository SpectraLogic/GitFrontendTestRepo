package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public class DeleteCacheThrottleRuleRequestHandler extends BaseDeleteBeanRequestHandler<CacheThrottleRule> {
    public DeleteCacheThrottleRuleRequestHandler() {
        super(CacheThrottleRule.class,
                new DefaultPublicExposureAuthenticationStrategy(DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication.ADMINISTRATOR),
                RestDomainType.CACHE_THROTTLE_RULE);
    }
}
