/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacementRuleState;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.dao.service.tape.TapeLibraryService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;

/**
 * This is for testing purposes only e.g. test clients may want to be able to create a data policy without a
 * real backend.  This will allow them to create a mocked tape backend they can create a storage domain 
 * using so that they can create the data policy to use.
 */
public final class CreateFakeTapeEnvironmentRequestHandler extends BaseRequestHandler
{
    public CreateFakeTapeEnvironmentRequestHandler()
    {
        super( new InternalAccessOnlyAuthenticationStrategy(),
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.CREATE, 
                       RestDomainType.TAPE_ENVIRONMENT ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setName( "fake" ).setManagementUrl( "fake" ).setSerialNumber( getClass().getSimpleName() );
        params.getServiceManager().getService( TapeLibraryService.class ).create( library );
        
        final TapePartition partition = BeanFactory.newBean( TapePartition.class )
                .setName( "fake" ).setImportExportConfiguration( ImportExportConfiguration.values()[ 0 ] )
                .setLibraryId( library.getId() ).setSerialNumber( getClass().getSimpleName() );
        params.getServiceManager().getService( TapePartitionService.class ).create( partition );
        
        final StorageDomain storageDomain = BeanFactory.newBean( StorageDomain.class )
                .setName( "fake" );
        params.getServiceManager().getService( StorageDomainService.class ).create( storageDomain );
        params.getDataPolicyResource().createStorageDomainMember( (StorageDomainMember)
                BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( storageDomain.getId() ).setTapeType( TapeType.LTO5 )
                .setTapePartitionId( partition.getId() )
                .setId( UUID.randomUUID() ) );
        
        final DataPolicy dataPolicy = BeanFactory.newBean( DataPolicy.class ).setName( "fake" );
        params.getServiceManager().getService( DataPolicyService.class ).create( dataPolicy );
        params.getDataPolicyResource().createDataPersistenceRule( (DataPersistenceRule)
                BeanFactory.newBean( DataPersistenceRule.class )
                .setIsolationLevel( DataIsolationLevel.STANDARD )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataPersistenceRuleType.PERMANENT )
                .setStorageDomainId( storageDomain.getId() )
                .setDataPolicyId( dataPolicy.getId() )
                .setId( UUID.randomUUID() ) );
        
        return BeanServlet.serviceCreate( params, dataPolicy );
    }
}
