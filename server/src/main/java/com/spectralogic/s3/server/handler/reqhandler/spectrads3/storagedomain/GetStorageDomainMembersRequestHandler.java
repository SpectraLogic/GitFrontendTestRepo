/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetStorageDomainMembersRequestHandler 
    extends BaseGetBeansRequestHandler< StorageDomainMember >
{
    public GetStorageDomainMembersRequestHandler()
    {
        super( StorageDomainMember.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.STORAGE_DOMAIN_MEMBER );
        
        registerOptionalBeanProperties(
                StorageDomainMember.POOL_PARTITION_ID,
                StorageDomainMember.STATE,
                StorageDomainMember.STORAGE_DOMAIN_ID,
                StorageDomainMember.TAPE_PARTITION_ID,
                StorageDomainMember.TAPE_TYPE,
                StorageDomainMember.WRITE_PREFERENCE );
    }
}
