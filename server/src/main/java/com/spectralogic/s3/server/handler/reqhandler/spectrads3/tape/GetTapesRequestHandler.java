/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public class GetTapesRequestHandler extends BaseGetBeansRequestHandler< Tape >
{
    public GetTapesRequestHandler()
    {
        super( Tape.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.TAPE );
        
        registerOptionalBeanProperties( 
                Tape.BAR_CODE,
                Tape.EJECT_LABEL,
                Tape.EJECT_LOCATION,
                Tape.FULL_OF_DATA,
                Tape.PARTITION_ID,
                Tape.STATE,
                Tape.PREVIOUS_STATE,
                Tape.TYPE,
                Tape.WRITE_PROTECTED,
                Tape.PARTIALLY_VERIFIED_END_OF_TAPE,
                Tape.VERIFY_PENDING,
                Tape.AVAILABLE_RAW_CAPACITY,
                SerialNumberObservable.SERIAL_NUMBER,
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.BUCKET_ID,
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.LAST_VERIFIED );
    
        registerOptionalRequestParameters(
                RequestParameterType.SORT_BY,
                RequestParameterType.BUCKET,
                RequestParameterType.STORAGE_DOMAIN,
                RequestParameterType.PARTITION
        );
    }

    @Override
    protected WhereClause getCustomFilter(Tape requestBean, CommandExecutionParams params) {
        WhereClause retval = Require.nothing();
        if (params.getRequest().hasRequestParameter( RequestParameterType.BUCKET ) ){
            final String bucketSearchString = params.getRequest().getRequestParameter( RequestParameterType.BUCKET ).getString();
            retval = Require.all(
                    retval,
                    Require.exists(
                            Tape.BUCKET_ID,
                            Require.beanPropertyMatches(
                                    Bucket.NAME,
                                    bucketSearchString)));

        }
        if (params.getRequest().hasRequestParameter( RequestParameterType.PARTITION)) {
            final String partitionSearchString = params.getRequest().getRequestParameter( RequestParameterType.PARTITION ).getString();
            retval = Require.all(
                    retval,
                    Require.exists(
                            Tape.PARTITION_ID,
                            Require.oneOfBeanPropertiesMatches(
                                    partitionSearchString,
                                    TapePartition.NAME, TapePartition.SERIAL_NUMBER)));
        }
        if (params.getRequest().hasRequestParameter( RequestParameterType.STORAGE_DOMAIN)) {
            final String storageDomainSearchString = params.getRequest().getRequestParameter( RequestParameterType.STORAGE_DOMAIN ).getString();
            retval = Require.all(
                    retval,
                    Require.exists(
                            Tape.STORAGE_DOMAIN_MEMBER_ID,
                            Require.exists(StorageDomainMember.STORAGE_DOMAIN_ID,
                                Require.beanPropertyMatches(
                                        StorageDomain.NAME,
                                        storageDomainSearchString))));
        }
        return retval;
    }
}
