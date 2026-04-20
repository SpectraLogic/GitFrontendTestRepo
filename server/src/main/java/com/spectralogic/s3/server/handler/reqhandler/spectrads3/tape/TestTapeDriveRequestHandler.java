/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.ReservedTaskType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeRole;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

import java.util.UUID;

public final class TestTapeDriveRequestHandler extends BaseRequestHandler
{
    public TestTapeDriveRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.TEST,
                       RestDomainType.TAPE_DRIVE ) );
        registerOptionalRequestParameters(
                RequestParameterType.TAPE_ID,
                RequestParameterType.SKIP_CLEAN
        );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final TapeDrive drive = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( TapeDrive.class ) );
        final UUID tapeId = params.getRequest().hasRequestParameter( RequestParameterType.TAPE_ID ) ? params.getRequest().getRequestParameter( RequestParameterType.TAPE_ID ).getUuid() : null;
        if (tapeId != null) {
            final Tape tape = params.getServiceManager().getRetriever( Tape.class ).attain(tapeId);
            if (tape.getRole() != TapeRole.TEST) {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "Tape " + tape.getBarCode() + " must be assigned the " + TapeRole.TEST + " role.");
            }
            if (!drive.getPartitionId().equals(tape.getPartitionId())) {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "Tape " + tape.getBarCode() + " and drive " + drive.getMfgSerialNumber() +
                                " are not in the same partition.");
            }
            if (tape.getState() != TapeState.NORMAL) {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "Tape " + tape.getBarCode() + " cannot be used because it is in state " + tape.getState() + ".");
            }
        } else {
            final int availableTestTapes = params.getServiceManager().getRetriever( Tape.class )
                    .getCount( Require.all(
                            Require.beanPropertyEquals( Tape.PARTITION_ID, drive.getPartitionId() ),
                            Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ),
                            Require.beanPropertyEquals( Tape.ROLE, TapeRole.TEST) ) );

            if (availableTestTapes <= 0) {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "No test tapes are available to test drive " + drive.getMfgSerialNumber() + ". " +
                                "Please designate a blank tape in this partition to have the " + TapeRole.TEST + " role.");
            }
        }

        if (drive.getReservedTaskType() != ReservedTaskType.MAINTENANCE) {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "Please put the drive into maintenance mode before testing.");
        }

        final boolean cleanFirst = !params.getRequest().hasRequestParameter( RequestParameterType.SKIP_CLEAN );
        params.getTapeResource().testDrive( drive.getId(), tapeId, cleanFirst ).get( Timeout.LONG );

        return BeanServlet.serviceModify(
                params, 
                params.getServiceManager().getRetriever( TapeDrive.class ).attain( drive.getId() ) );
    }
}
