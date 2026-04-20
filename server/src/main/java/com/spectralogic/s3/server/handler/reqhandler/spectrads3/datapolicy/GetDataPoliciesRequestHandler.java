/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.handler.auth.DataPolicyAuthorization;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetDataPoliciesRequestHandler extends BaseGetBeansRequestHandler< DataPolicy >
{
    public GetDataPoliciesRequestHandler()
    {
        super( DataPolicy.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.DATA_POLICY );
        
        registerOptionalBeanProperties(
                NameObservable.NAME,
                DataPolicy.CHECKSUM_TYPE,
                DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.ALWAYS_FORCE_PUT_JOB_CREATION,
                DataPolicy.ALWAYS_MINIMIZE_SPANNING_ACROSS_MEDIA );
    }

    
    @Override
    protected WhereClause getCustomFilter( final DataPolicy requestBean, final CommandExecutionParams params )
    {
        return Require.beanPropertyEqualsOneOf(
                Identifiable.ID, DataPolicyAuthorization.getDataPoliciesUserHasAccessTo( params ) );
    }
}
