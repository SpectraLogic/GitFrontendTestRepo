/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeRole;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;

import java.util.Set;

public final class ModifyTapeRequestHandler extends BaseModifyBeanRequestHandler< Tape >
{
    public ModifyTapeRequestHandler()
    {
        super( Tape.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.TAPE );

        registerOptionalBeanProperties(
               Tape.EJECT_LABEL,
               Tape.EJECT_LOCATION,
               Tape.STATE,
               Tape.ROLE ,
               Tape.ALLOW_ROLLBACK);
    }


    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final Tape specifiedTape,
            final Set< String > modifiedProperties )
    {
        final Tape tape =
                params.getServiceManager().getRetriever( Tape.class ).attain( specifiedTape.getId() );
        if ( !modifiedProperties.contains( Tape.STATE ) && !modifiedProperties.contains( Tape.ROLE ))
        {
            return;
        }

        if (modifiedProperties.contains(Tape.ROLE)) {
            final BeansRetriever<Tape> retriever = params.getServiceManager().getRetriever( Tape.class );
            final Tape existingTape = retriever.attain(specifiedTape.getId());
            if ( specifiedTape.getRole() != existingTape.getRole() ) {
                if (specifiedTape.getRole() == TapeRole.NORMAL) {
                    //The tape is being returned to normal, so flag it "assigned to storage domain" to assure it
                    //gets auto reclaimed prior to having any managed data written to it.
                    specifiedTape.setAssignedToStorageDomain(true);
                    modifiedProperties.add( Tape.ASSIGNED_TO_STORAGE_DOMAIN );
                } else if (specifiedTape.getRole() == TapeRole.TEST) {
                    if (existingTape.getState() != TapeState.NORMAL) {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Tape " + existingTape.getBarCode() + " cannot be set to role " + specifiedTape.getRole() + " because it is in state " + existingTape.getState() + ".");
                    }
                    if (existingTape.getStorageDomainMemberId() != null) {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Tape " + existingTape.getBarCode() + " cannot be set to role " + specifiedTape.getRole() + " because it is allocated to a storage domain. Please select a blank, unallocated tape." );
                    }
                    if (existingTape.isAssignedToStorageDomain()) {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Tape " + existingTape.getBarCode() + " cannot be set to role " + specifiedTape.getRole() + " because it is pending reclaim and needs to be formatted." );
                    }
                    if (existingTape.getBucketId() != null) {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Tape " + existingTape.getBarCode() + " cannot be set to role " + specifiedTape.getRole() + " because it is allocated to a bucket. Please select a blank, unallocated tape." );
                    }
                    if (existingTape.isFullOfData()) {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Tape " + existingTape.getBarCode() + " cannot be set to role " + specifiedTape.getRole() + " because it is marked as \"full of data\". Please select a blank, unallocated tape." );
                    }
                    final TapePartition partition = params.getServiceManager().getRetriever(TapePartition.class).attain(existingTape.getPartitionId());
                    if (!partition.getDriveType().isWriteSupported(existingTape.getType())) {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Tape " + existingTape.getBarCode() + " cannot be set to role " + specifiedTape.getRole() + " because a partition with drive type "
                                        + partition.getDriveType() + " cannot write to a tape of type " + existingTape.getType() + ". Please select a writeable tape." );
                    }
                }
            }
        } else {
            if ( !m_allowedStatesToTransistBetween.contains( specifiedTape.getState() ) )
            {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "You can only transist a tape's state to one of these states: "
                                + m_allowedStatesToTransistBetween );
            }
            if ( !m_allowedStatesToTransistBetween.contains( tape.getState() ) )
            {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "You can only transist a tape's state from one of these states: "
                                + m_allowedStatesToTransistBetween );
            }

            params.getServiceManager().getService( TapeService.class ).transistState(
                    tape, specifiedTape.getState() );
            modifiedProperties.remove( Tape.STATE );
        }

    }


    private final Set< TapeState > m_allowedStatesToTransistBetween =
            CollectionFactory.toSet( TapeState.EJECTED, TapeState.LOST );
}
