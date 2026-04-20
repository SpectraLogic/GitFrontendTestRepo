package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.GenericFailure;

public class CreateCacheThrottleRuleRequestHandler extends BaseCreateBeanRequestHandler<CacheThrottleRule> {
    public CreateCacheThrottleRuleRequestHandler() {
        super(CacheThrottleRule.class,
                new DefaultPublicExposureAuthenticationStrategy(DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication.ADMINISTRATOR),
                RestDomainType.CACHE_THROTTLE_RULE);

        registerBeanProperties(
                CacheThrottleRule.PRIORITY,
                CacheThrottleRule.BUCKET_ID,
                CacheThrottleRule.REQUEST_TYPE,
                CacheThrottleRule.MAX_CACHE_PERCENT,
                CacheThrottleRule.BURST_THRESHOLD);
    }

    @Override
    protected void validateBeanForCreation(final CommandExecutionParams params, final CacheThrottleRule rule)
    {
        validate(params, rule);
    }

    static void validate(final CommandExecutionParams params, final CacheThrottleRule rule) {
        if (rule.getBucketId() == null && rule.getRequestType() == null & rule.getPriority() == null) {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    "Must specify at leat one: " + CacheThrottleRule.BUCKET_ID + ", " + CacheThrottleRule.REQUEST_TYPE + ", " + CacheThrottleRule.PRIORITY);
        }

        if (rule.getMaxCachePercent() > 1.0 || rule.getMaxCachePercent() <= 0.0) {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    CacheThrottleRule.MAX_CACHE_PERCENT + " is '" + rule.getMaxCachePercent()
                            + "' but must be a percentage value greater than 0 and less than 1.0"
            );
        }

        final WhereClause whereClause = Require.all(
                Require.beanPropertyEquals(CacheThrottleRule.BUCKET_ID, rule.getBucketId()),
                Require.beanPropertyEquals(CacheThrottleRule.PRIORITY, rule.getPriority()),
                Require.beanPropertyEquals(CacheThrottleRule.REQUEST_TYPE, rule.getRequestType()),
                Require.not(Require.beanPropertyEquals(CacheThrottleRule.ID, rule.getId()))
        );
        final boolean alreadyExists = params.getServiceManager().getRetriever(CacheThrottleRule.class).any(whereClause);
        if (alreadyExists) {
            throw new S3RestException(
                    GenericFailure.CONFLICT,
                    "A rule already exists with " + CacheThrottleRule.BUCKET_ID + " '" + rule.getBucketId() + "' "
                            + ", " + CacheThrottleRule.REQUEST_TYPE + " '" + rule.getRequestType() + "' "
                            + ", " + CacheThrottleRule.PRIORITY + " '" + rule.getPriority() + "' ");
        }
    }
}
