package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
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
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

public class ModifyTapeDriveRequestHandler extends BaseModifyBeanRequestHandler< TapeDrive >
{
    public ModifyTapeDriveRequestHandler()
    {
        super( TapeDrive.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               RestDomainType.TAPE_DRIVE );
      
        registerOptionalBeanProperties(
                TapeDrive.QUIESCED );
        registerOptionalBeanProperties(
                TapeDrive.RESERVED_TASK_TYPE );
        registerOptionalBeanProperties(
                TapeDrive.MINIMUM_TASK_PRIORITY );
        registerOptionalBeanProperties(
                TapeDrive.MAX_FAILED_TAPES );
    }

  
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final TapeDrive bean,
            final Set< String > modifiedProperties )
    {
        final BeansRetriever<TapeDrive> driveRetriever =
                params.getServiceManager().getRetriever( TapeDrive.class );
        final BeansRetriever<TapePartition> partitionRetriever =
                params.getServiceManager().getRetriever( TapePartition.class );

        if ( modifiedProperties.contains( TapeDrive.QUIESCED ) )
        {
            validateQuiescedValueChange( driveRetriever, bean );
        }

        if ( modifiedProperties.contains( TapeDrive.RESERVED_TASK_TYPE ) || modifiedProperties.contains( TapeDrive.MINIMUM_TASK_PRIORITY ) )
        {
            validateTaskReservationValuesChange( driveRetriever, partitionRetriever, bean );
        }

        if ( modifiedProperties.contains( TapeDrive.MAX_FAILED_TAPES ) )
        {
            if (bean.getMaxFailedTapes() != null && bean.getMaxFailedTapes() <= 0) {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "Max failed tapes must be greater than 0 if specified.");
            }
        }
    }
  
  
    protected static void validateQuiescedValueChange(
            final BeansRetriever< TapeDrive > driveRetriever,
            final TapeDrive bean )
    {
        final TapeDrive drive =
                driveRetriever.attain( bean.getId() );
      
        if ( drive.getQuiesced().equals( bean.getQuiesced() )
                || Quiesced.NO == bean.getQuiesced()
                || Quiesced.PENDING == bean.getQuiesced() )
        {
            return;
        }
      
        throw new FailureTypeObservableException(
                GenericFailure.BAD_REQUEST,
                "It is illegal to transist the quiesced state from " + drive.getQuiesced()
                + " to " + bean.getQuiesced() + "." );
    }

    protected static void validateTaskReservationValuesChange(
            final BeansRetriever< TapeDrive > driveRetriever,
            final BeansRetriever< TapePartition > partitionRetriever,
            final TapeDrive bean )
    {
        final TapeDrive drive =
                driveRetriever.attain( bean.getId() );
        final TapePartition partition =
                partitionRetriever.attain( bean.getPartitionId() );

        //NOTE: Objects.equals used for null safety
        final boolean changingTaskType = !Objects.equals( drive.getReservedTaskType(), ( bean.getReservedTaskType() ) );
        final boolean changingMinTaskPriority = !Objects.equals( drive.getMinimumTaskPriority(), bean.getMinimumTaskPriority() );
        final boolean taskTypeValidationNeeded = changingTaskType
                        && bean.getReservedTaskType() != ReservedTaskType.ANY
                        && bean.getReservedTaskType() != ReservedTaskType.MAINTENANCE;
        final boolean priorityValidationNeeded = changingMinTaskPriority
                        && bean.getMinimumTaskPriority() != null;
        if ( !taskTypeValidationNeeded && !priorityValidationNeeded)
        {
            return;
        }

        final Integer totalDriveCount = driveRetriever.getCount(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, bean.getPartitionId() ) );
        final Integer readDriveCount = driveRetriever.getCount( Require.all(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, bean.getPartitionId() ),
                Require.beanPropertyEquals( TapeDrive.RESERVED_TASK_TYPE, ReservedTaskType.READ) ) );
        final Integer writeDriveCount = driveRetriever.getCount( Require.all(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, bean.getPartitionId() ),
                Require.beanPropertyEquals( TapeDrive.RESERVED_TASK_TYPE, ReservedTaskType.WRITE) ) );
        Integer maxReadCount = readDriveCount;
        Integer maxWriteCount = writeDriveCount;

        if ( partition.getMinimumReadReservedDrives() > maxReadCount )
        {
            maxReadCount = partition.getMinimumReadReservedDrives();
        }
        if ( partition.getMinimumWriteReservedDrives() > maxWriteCount )
        {
            maxWriteCount = partition.getMinimumWriteReservedDrives();
        }

        if ( bean.getReservedTaskType() == ReservedTaskType.READ && changingTaskType)
        {
            if ( readDriveCount + 1 == totalDriveCount )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the reserved task type to READ because every other tape drive in " +
                        "this tape drive's tape partition has its reserved task type set as READ.");
            }
            if ( readDriveCount + 1 > totalDriveCount - maxWriteCount )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the reserved task type to READ because that would violate the required " +
                        "number of tape drives in this tape drive's tape partition reserved for WRITE tasks.");
            }
        }
        else if ( bean.getReservedTaskType() == ReservedTaskType.WRITE && changingTaskType)
        {
            if ( writeDriveCount + 1 == totalDriveCount )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the reserved task type to WRITE because every other tape drive in " +
                        "this tape drive's tape partition has its reserved task type set as WRITE.");
            }
            if ( writeDriveCount + 1 > totalDriveCount - maxReadCount )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "It is illegal to set the reserved task type to WRITE because that would violate the required " +
                        "number of tape drives in this tape drive's tape partition reserved for READ tasks.");
            }
        }

        validateReadWritePossibleForAllPriorities(driveRetriever, bean);
    }

    private static void validateReadWritePossibleForAllPriorities(final BeansRetriever<TapeDrive> driveRetriever, final TapeDrive bean) {
        final TapeDrive drive = driveRetriever.retrieve(bean.getId());
        final List<TapeDrive> canReadAllPriorities = driveRetriever.retrieveAll( Require.all(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, drive.getPartitionId() ),
                Require.beanPropertyEquals( TapeDrive.MINIMUM_TASK_PRIORITY, null ),
                Require.beanPropertyEqualsOneOf(TapeDrive.RESERVED_TASK_TYPE, ReservedTaskType.READ, ReservedTaskType.ANY) ) ).toList();
        final List<TapeDrive> canWriteAllPriorities = driveRetriever.retrieveAll( Require.all(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, drive.getPartitionId() ),
                Require.beanPropertyEquals( TapeDrive.MINIMUM_TASK_PRIORITY, null ),
                Require.beanPropertyEqualsOneOf(TapeDrive.RESERVED_TASK_TYPE, ReservedTaskType.WRITE, ReservedTaskType.ANY) ) ).toList();

        final boolean onlyDriveThatCanReadAtAllPriorities = canReadAllPriorities.size() == 1 && canReadAllPriorities.get( 0 ).getId().equals(bean.getId());
        final boolean canNoLongerReadAtAllPriorities = bean.getReservedTaskType() == ReservedTaskType.WRITE || bean.getMinimumTaskPriority() != null;

        final boolean onlyDriveThatCanWriteAtAllPriorities = canWriteAllPriorities.size() == 1 && canWriteAllPriorities.get( 0 ).getId().equals(bean.getId());
        final boolean canNoLongerWriteAtAllPriorities = bean.getReservedTaskType() == ReservedTaskType.READ || bean.getMinimumTaskPriority() != null;

        if (onlyDriveThatCanReadAtAllPriorities &&  canNoLongerReadAtAllPriorities) {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "Cannot make change to drive's reserved task type or minimum priority because it is currently " +
                            "the only drive allowed to read at the lowest priority for this partition.");
        }

        if (onlyDriveThatCanWriteAtAllPriorities && canNoLongerWriteAtAllPriorities) {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "Cannot make change to drive's reserved task type or minimum priority because it is currently " +
                            "the only drive allowed to write at the lowest priority for this partition.");
        }
    }
}
