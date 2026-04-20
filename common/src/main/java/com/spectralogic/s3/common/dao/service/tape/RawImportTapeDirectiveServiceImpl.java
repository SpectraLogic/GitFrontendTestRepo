/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ImportTapeTargetDirective;
import com.spectralogic.s3.common.dao.domain.tape.ImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

final class RawImportTapeDirectiveServiceImpl
    extends BaseService< RawImportTapeDirective > implements RawImportTapeDirectiveService
{
    RawImportTapeDirectiveServiceImpl()
    {
        super( RawImportTapeDirective.class );
    }


    public RawImportTapeDirective attainByEntityToImport( final UUID idOfEntityToImport )
    {
        return attain( ImportTapeTargetDirective.TAPE_ID, idOfEntityToImport );
    }


    public void deleteByEntityToImport( final UUID tapeId )
    {
        getDataManager().deleteBeans(
                getServicedType(),
                Require.beanPropertyEquals( ImportTapeTargetDirective.TAPE_ID, tapeId ) );
    }


    @Override
    public void deleteAll()
    {
        if ( 0 < getServiceManager().getRetriever( Tape.class ).getCount( Require.beanPropertyEqualsOneOf( 
                Tape.STATE, TapeState.RAW_IMPORT_IN_PROGRESS, TapeState.RAW_IMPORT_PENDING ) ) )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "Can only delete all " + ImportTapeDirective.class.getSimpleName() 
                    + "s if no tapes are pending import or import in progress." );
        }
        getDataManager().deleteBeans( getServicedType(), Require.nothing() );
    }
}
