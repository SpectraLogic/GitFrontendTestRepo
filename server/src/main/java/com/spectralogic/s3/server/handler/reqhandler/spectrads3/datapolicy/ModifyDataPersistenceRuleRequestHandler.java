/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyDataPersistenceRuleRequestHandler
    extends BaseModifyBeanRequestHandler< DataPersistenceRule >
{
    public ModifyDataPersistenceRuleRequestHandler()
    {
        super( DataPersistenceRule.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DATA_PERSISTENCE_RULE );
        
        registerOptionalBeanProperties(
                DataPersistenceRule.ISOLATION_LEVEL,
                DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN,
                DataPersistenceRule.TYPE );
    }

    
    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final DataPersistenceRule rule,
            final Set< String > modifiedProperties )
    {
        if ( modifiedProperties.contains( DataPersistenceRule.TYPE ) 
                && DataPersistenceRuleType.TEMPORARY != rule.getType()
                && !modifiedProperties.contains( DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN )
                && null != rule.getMinimumDaysToRetain() )
        {
            LOG.info( "It is implied that " + DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN
                      + " should be set to null as it no longer applies." );
            modifiedProperties.add( DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
            rule.setMinimumDaysToRetain( null );
        }
        
        CreateDataPersistenceRuleRequestHandler.validate( rule );
        params.getDataPolicyResource().modifyDataPersistenceRule(
                rule,
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.LONG );
    }
}
