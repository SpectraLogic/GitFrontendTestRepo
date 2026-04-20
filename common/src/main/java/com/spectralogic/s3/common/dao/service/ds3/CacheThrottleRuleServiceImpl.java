package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.util.db.service.BaseService;

final class CacheThrottleRuleServiceImpl extends BaseService<CacheThrottleRule> implements CacheThrottleRuleService {
    CacheThrottleRuleServiceImpl() {
        super(CacheThrottleRule.class);
    }
}
