/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.shared.ReservedTaskType;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;

public final class ModifyTapePartitionRequestHandler extends BaseModifyBeanRequestHandler< TapePartition >
{
    public ModifyTapePartitionRequestHandler()
    {
        super( TapePartition.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               RestDomainType.TAPE_PARTITION );
        
        registerOptionalBeanProperties( 
                TapePartition.QUIESCED,
                TapePartition.MINIMUM_READ_RESERVED_DRIVES,
                TapePartition.MINIMUM_WRITE_RESERVED_DRIVES,
                TapePartition.AUTO_COMPACTION_ENABLED,
                TapePartition.AUTO_QUIESCE_ENABLED,
                TapePartition.DRIVE_IDLE_TIMEOUT_IN_MINUTES,
                TapePartition.SERIAL_NUMBER );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final TapePartition bean,
            final Set< String > modifiedProperties )
    {
        validateBeanToCommit( params, bean, modifiedProperties );
        final BeanUpdater< TapePartition > updater = params.getServiceManager().getUpdater( TapePartition.class );
        updater.update( bean, CollectionFactory.toArray( String.class, modifiedProperties ) );
        if ( modifiedProperties.contains( TapePartition.QUIESCED ) && bean.getQuiesced() == Quiesced.NO ) {
            //We just unquiesced this partition, flag environment for refresh
            params.getTapeResource().flagEnvironmentForRefresh();
        }

    }
    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final TapePartition bean,
            final Set< String > modifiedProperties )
    {
        final BeansRetriever<TapePartition> partitionRetriever = 
                params.getServiceManager().getRetriever( TapePartition.class );
        final BeansRetriever<TapeDrive> tapeDriveRetriever =
                params.getServiceManager().getRetriever( TapeDrive.class );
        
        if ( modifiedProperties.contains( TapePartition.QUIESCED ) ) 
        { 
            validateQuiescedValueChange( partitionRetriever, bean );
        }

        if ( modifiedProperties.contains( TapePartition.MINIMUM_READ_RESERVED_DRIVES ) ||
                modifiedProperties.contains( TapePartition.MINIMUM_WRITE_RESERVED_DRIVES ) )
        {
            validateReservedDriveValueChange( partitionRetriever, tapeDriveRetriever, bean, modifiedProperties );
        }

        if ( modifiedProperties.contains( TapePartition.DRIVE_IDLE_TIMEOUT_IN_MINUTES ) )
        {
            if ( bean.getDriveIdleTimeoutInMinutes() != null && bean.getDriveIdleTimeoutInMinutes() <= 0 )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "Drive idle timeout in minutes must be a positive number."
                );
            }
        }
    }
    
    
    protected static void validateQuiescedValueChange( 
            final BeansRetriever< TapePartition > partitionRetriever, 
            final TapePartition bean ) 
    {
        final TapePartition partition = 
                partitionRetriever.attain( bean.getId() );
        
        if ( partition.getQuiesced().equals( bean.getQuiesced() )
                || Quiesced.NO == bean.getQuiesced()
                || Quiesced.PENDING == bean.getQuiesced() )
        {
            return;
        }
        
        throw new FailureTypeObservableException( 
                GenericFailure.BAD_REQUEST,
                "It is illegal to transist the quiesced state from " + partition.getQuiesced()
                + " to " + bean.getQuiesced() + "." );
    }

    protected static void validateReservedDriveValueChange(
            final BeansRetriever<TapePartition> partitionRetriever,
            final BeansRetriever<TapeDrive> tapeDriveRetriever,
            final TapePartition bean,
            final Set<String> modifiedProperties)
    {
        final TapePartition partition =
                partitionRetriever.attain( bean.getId() );
        final boolean mustValidateReadMin = partition.getMinimumReadReservedDrives() != bean.getMinimumReadReservedDrives()
                && bean.getMinimumReadReservedDrives() != 0;
        final boolean mustValidateWriteMin = partition.getMinimumWriteReservedDrives() != bean.getMinimumWriteReservedDrives()
                && bean.getMinimumWriteReservedDrives() != 0;
        if ( !mustValidateReadMin && !mustValidateWriteMin)
        {
            return;
        }

        final Integer totalDriveCount = tapeDriveRetriever.getCount(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, bean.getId() ) );
        Integer runningCount = totalDriveCount;

        final Integer readReservedCount = tapeDriveRetriever.getCount( Require.all(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, bean.getId() ),
                Require.beanPropertyEquals( TapeDrive.RESERVED_TASK_TYPE, ReservedTaskType.READ) ) );
        final Integer writeReservedCount = tapeDriveRetriever.getCount( Require.all(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, bean.getId() ),
                Require.beanPropertyEquals( TapeDrive.RESERVED_TASK_TYPE, ReservedTaskType.WRITE ) ) );

        final int resultantReadMinimum;
        if (modifiedProperties.contains( TapePartition.MINIMUM_READ_RESERVED_DRIVES )) {
            resultantReadMinimum = bean.getMinimumReadReservedDrives();
        } else {
            resultantReadMinimum = partition.getMinimumReadReservedDrives();
        }
        final int resultantWriteMinimum;
        if (modifiedProperties.contains( TapePartition.MINIMUM_WRITE_RESERVED_DRIVES )) {
            resultantWriteMinimum = bean.getMinimumWriteReservedDrives();
        } else {
            resultantWriteMinimum = partition.getMinimumWriteReservedDrives();
        }

        if (resultantReadMinimum != 0 && resultantWriteMinimum != 0) {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "It is illegal to set the minimum reserved drives for both reads and writes on the same " +
                            "partition. One or the other must be set to zero." );
        }

        if ( 0 < bean.getMinimumReadReservedDrives() )
        {
            if ( bean.getMinimumReadReservedDrives() == totalDriveCount )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the minimum read reserved drives value to the total number of " +
                        "tape drives in the tape partition." );
            }

            if ( totalDriveCount - partition.getMinimumReadReservedDrives() <= 0 )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the minimum read reserved drives value to any number higher than the " +
                        "total number of tape drives in the tape partition." );
            }
        }

        if ( bean.getMinimumReadReservedDrives() > readReservedCount )
        {
            runningCount -= bean.getMinimumReadReservedDrives();
        }
        else
        {
            runningCount -= readReservedCount;
        }

        if ( 0 < bean.getMinimumWriteReservedDrives() )
        {
            if ( bean.getMinimumWriteReservedDrives() == totalDriveCount )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the minimum write reserved drives value to the total number of " +
                        "tape drives in the tape partition." );
            }

            if ( totalDriveCount - partition.getMinimumWriteReservedDrives() <= 0 )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the minimum write reserved drives value to any number higher than 0 if there are any " +
                        "tape drives in the tape partition set to be reserved for writes." );
            }
        }
        if ( bean.getMinimumWriteReservedDrives() > writeReservedCount )
        {
            runningCount -= bean.getMinimumWriteReservedDrives();
        }
        else
        {
            runningCount -= writeReservedCount;
        }

        if ( runningCount < 0 )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "The sum of minimum reserved drives and/or specifically reserved drives cannot exceed " +
                    "the total number of tape drives in the tape partition." );
        }
    }
}
