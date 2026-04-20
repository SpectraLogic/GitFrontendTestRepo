package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.CacheThrottleRule;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface CacheThrottleRuleService extends BeansRetriever<CacheThrottleRule>, BeanCreator<CacheThrottleRule>, BeanUpdater<CacheThrottleRule>, BeanDeleter {
    // empty
}
