/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetPoolsRequestHandler extends BaseGetBeansRequestHandler< Pool >
{
    public GetPoolsRequestHandler()
    {
        super( Pool.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.POOL );
        
        registerOptionalBeanProperties(
                NameObservable.NAME,
                Pool.PARTITION_ID, 
                Pool.STATE, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.BUCKET_ID,
                PersistenceTarget.LAST_VERIFIED,
                PoolObservable.HEALTH,
                PoolObservable.TYPE,
                PoolObservable.POWERED_ON,
                PoolObservable.GUID );
    }
}
