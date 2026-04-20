/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateTapeStorageDomainMemberRequestHandler
    extends BaseCreateBeanRequestHandler< StorageDomainMember >
{
    public CreateTapeStorageDomainMemberRequestHandler()
    {
        super( StorageDomainMember.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.STORAGE_DOMAIN_MEMBER );
        
        registerRequiredBeanProperties(
                StorageDomainMember.STORAGE_DOMAIN_ID,
                StorageDomainMember.TAPE_PARTITION_ID,
                StorageDomainMember.TAPE_TYPE );
        registerOptionalBeanProperties(
                StorageDomainMember.WRITE_PREFERENCE,
                StorageDomainMember.AUTO_COMPACTION_THRESHOLD );
    }


    @Override
    protected UUID createBean( final CommandExecutionParams params, final StorageDomainMember bean )
    {
        bean.setId( UUID.randomUUID() );
        if ( null != bean.getAutoCompactionThreshold() )
        {
            validateAutoCompactionThreshold( bean );
        }
        if ( null != bean.getWritePreference() )
        {
            validateWritePreference( bean, params.getServiceManager() );
        }
        return params.getDataPolicyResource().createStorageDomainMember( bean ).get( Timeout.LONG );
    }


    protected void validateAutoCompactionThreshold( final StorageDomainMember member )
    {
        if ( member.getAutoCompactionThreshold() < 0 || member.getAutoCompactionThreshold() > 100 )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "Auto Compaction Threshold must be an integer between 0 and 100." );
        }
    }
    
    
    private void validateWritePreference( final StorageDomainMember member, final BeansServiceManager bsm )
    {
        final Map< UUID, TapePartition > tapePartitions = BeanUtils.toMap( bsm.getRetriever( TapePartition.class )
                                                                              .retrieveAll()
                                                                              .toSet() );
        final TapePartition tapePartition = tapePartitions.get( member.getTapePartitionId() );
    
        if ( ( null == tapePartition ) || ( null == tapePartition.getDriveType() ) )
        {
            return;
        }
    
        if ( !tapePartition.getDriveType()
                           .isReadSupported( member.getTapeType() ) )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "Cannot create a storage domain member that can not read from its tapes." );
        }
        if ( ( WritePreferenceLevel.NEVER_SELECT == member.getWritePreference() ) || tapePartition.getDriveType()
                                                                                                  .isWriteSupported(
                                                                                                          member.getTapeType() ) )
        {
            return;
        }
        throw new S3RestException( GenericFailure.BAD_REQUEST,
                "Cannot create a storage domain member that can only read from its tapes with a write preference" +
                        " other than " + WritePreferenceLevel.NEVER_SELECT.toString() + "." );
    }
}
