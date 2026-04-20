/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyStorageDomainMemberRequestHandler
    extends BaseModifyBeanRequestHandler< StorageDomainMember >
{
    public ModifyStorageDomainMemberRequestHandler()
    {
        super( StorageDomainMember.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.STORAGE_DOMAIN_MEMBER );
        
        registerOptionalBeanProperties( 
                StorageDomainMember.STATE,
                StorageDomainMember.WRITE_PREFERENCE,
                StorageDomainMember.AUTO_COMPACTION_THRESHOLD );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final StorageDomainMember bean,
            final Set< String > modifiedProperties )
    {
        params.getDataPolicyResource().modifyStorageDomainMember(
                bean, 
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.LONG );
    }


    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final StorageDomainMember member,
            final Set< String > modifiedProperties )
    {
        if ( modifiedProperties.contains( StorageDomainMember.AUTO_COMPACTION_THRESHOLD ) &&
                null != member.getAutoCompactionThreshold() )
        {
            validateAutoCompactionThreshold( member );
        }
        if ( modifiedProperties.contains( StorageDomainMember.WRITE_PREFERENCE ) &&
                null != member.getWritePreference() )
        {
            validateWritePreference( member, params.getServiceManager() );
        }
    }


    private void validateAutoCompactionThreshold( final StorageDomainMember member )
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
        
        if ( ( WritePreferenceLevel.NEVER_SELECT == member.getWritePreference() ) || ( null == tapePartition ) ||
                ( null == tapePartition.getDriveType() ) )
        {
            return;
        }
        
        if ( !tapePartition.getDriveType()
                           .isWriteSupported( member.getTapeType() ) )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "Cannot modify write preference since the storage domain member cannot write to the tapes." );
        }
    }
}
