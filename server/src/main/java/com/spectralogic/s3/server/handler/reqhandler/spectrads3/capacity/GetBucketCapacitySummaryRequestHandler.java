/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.capacity;

import com.spectralogic.s3.common.dao.domain.ds3.CapacitySummaryContainer;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetBucketCapacitySummaryRequestHandler 
    extends BaseDaoTypedRequestHandler< CapacitySummaryParams >
{
    public GetBucketCapacitySummaryRequestHandler()
    {
        super( CapacitySummaryParams.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.LIST,
                       RestDomainType.CAPACITY_SUMMARY ) );
        
        registerRequiredBeanProperties( 
                CapacitySummaryRequiredParams.STORAGE_DOMAIN_ID,
                CapacitySummaryRequiredParams.BUCKET_ID );
        registerOptionalBeanProperties( CollectionFactory.toArray(
                String.class,
                BeanUtils.getPropertyNames( CapacitySummaryOptionalParams.class ) ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final CapacitySummaryParams qParams = getBeanSpecifiedViaQueryParameters(
                params, AutoPopulatePropertiesWithDefaults.YES );
        final CapacitySummaryContainer retval =
                params.getServiceManager().getService( StorageDomainService.class ).getCapacitySummary(
                        qParams.getBucketId(),
                        qParams.getStorageDomainId(),
                        CapacitySummaryFilter.forTape( qParams ),
                        CapacitySummaryFilter.forPool( qParams ) );
        
        return BeanServlet.serviceGet( 
                params, 
                retval );
    }
}
