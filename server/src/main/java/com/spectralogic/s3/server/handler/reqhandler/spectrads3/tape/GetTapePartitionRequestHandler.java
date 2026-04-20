/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.*;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.server.domain.DetailedTapePartition;
import com.spectralogic.s3.server.domain.TapeStateSummaryApiBean;
import com.spectralogic.s3.server.domain.TapeTypeSummaryApiBean;
import com.spectralogic.s3.server.domain.TypeTypeCountApiBean;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetTapePartitionRequestHandler extends BaseGetBeanRequestHandler< TapePartition >
{
    public GetTapePartitionRequestHandler()
    {
        super( TapePartition.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.TAPE_PARTITION );
    }
    

    @Override
    protected TapePartition performCustomPopulationWork(
            final DS3Request request,
            final CommandExecutionParams params,
            final TapePartition partition )
    {
        return getTapePartition( DetailedTapePartition.class, params, partition );
    }
    
    
    static TapePartition getTapePartition(
            final Class< ? extends DetailedTapePartition > clazz,
            final CommandExecutionParams params, 
            final TapePartition partition )
    {
        if ( !params.getRequest().hasRequestParameter( RequestParameterType.FULL_DETAILS ) )
        {
            return partition;
        }
        
        final DetailedTapePartition retval = BeanFactory.newBean( clazz );
        BeanCopier.copy( retval, partition );

        final Set<Tape> tapes = params.getServiceManager().getRetriever( Tape.class ).retrieveAll(
                Tape.PARTITION_ID, partition.getId() ).toSet();

        final List< TapeType > tapeTypes = new ArrayList<>();
        for ( final Tape tape : tapes )
        {
            if ( !tapeTypes.contains( tape.getType() ) )
            {
                tapeTypes.add( tape.getType() );
            }
        }
        Collections.sort( tapeTypes );
        retval.setTapeTypes( CollectionFactory.toArray( TapeType.class, tapeTypes ) );
        
        final List< TapeDriveType > driveTypes = new ArrayList<>();
        for ( final TapeDrive drive : params.getServiceManager().getRetriever( TapeDrive.class ).retrieveAll(
                Tape.PARTITION_ID, partition.getId() ).toSet() )
        {
            if ( !driveTypes.contains( drive.getType() ) )
            {
                driveTypes.add( drive.getType() );
            }
        }
        Collections.sort( driveTypes );
        retval.setDriveTypes( CollectionFactory.toArray( TapeDriveType.class, driveTypes ) );


        final long totalStorage = tapes.stream().collect(Collectors.summingLong((t) -> t.getTotalRawCapacity() == null ? 0 : t.getTotalRawCapacity()));
        final long availableStorage = tapes.stream().collect(Collectors.summingLong((t) -> t.getAvailableRawCapacity() == null ? 0 : t.getAvailableRawCapacity()));
        final long usedStorage = totalStorage - availableStorage;
        final List<TapeTypeSummaryApiBean> tapeTypeDetailsList = new ArrayList<>();
        final List<TapeStateSummaryApiBean> tapeStateDetailsList = new ArrayList<>();

        tapes.stream().collect(Collectors.groupingBy((t) -> t.getType())).entrySet().stream().forEach((e) -> {
            final TapeTypeSummaryApiBean tapeTypeDetails = BeanFactory.newBean( TapeTypeSummaryApiBean.class );
            tapeTypeDetails.setType( e.getKey() );
            tapeTypeDetails.setCount( e.getValue().size() );
            tapeTypeDetails.setTotalStorageCapacity( e.getValue().stream().collect(Collectors.summingLong((t) -> t.getTotalRawCapacity() == null ? 0 : t.getTotalRawCapacity())) );
            tapeTypeDetails.setAvailableStorageCapacity( e.getValue().stream().collect(Collectors.summingLong((t) -> t.getAvailableRawCapacity() == null ? 0 : t.getAvailableRawCapacity())) );
            tapeTypeDetails.setUsedStorageCapacity( tapeTypeDetails.getTotalStorageCapacity() - tapeTypeDetails.getAvailableStorageCapacity() );
            tapeTypeDetailsList.add(tapeTypeDetails);
        });

        tapes.stream().collect(Collectors.groupingBy((t) -> t.getState())).entrySet().stream().forEach((e) -> {
            final TapeStateSummaryApiBean tapeStateDetails = BeanFactory.newBean( TapeStateSummaryApiBean.class );
            tapeStateDetails.setTapeState( e.getKey() );
            tapeStateDetails.setCount( e.getValue().size() );
            tapeStateDetails.setTypeCounts( e.getValue().stream().collect(Collectors.groupingBy((t) -> t.getType())).entrySet().stream().map((e2) -> {
                final TypeTypeCountApiBean typeTypeCount = BeanFactory.newBean(TypeTypeCountApiBean.class);
                typeTypeCount.setType(e2.getKey());
                typeTypeCount.setCount(e2.getValue().size());
                typeTypeCount.setFullOfData(e2.getValue().stream().collect(Collectors.partitioningBy((t) -> t.isFullOfData())).get(true).size());
                return typeTypeCount;
            }).toArray(TypeTypeCountApiBean[]::new));
            tapeStateDetails.setFullOfData( e.getValue().stream().collect(Collectors.partitioningBy((t) -> t.isFullOfData())).get(true).size());
            tapeStateDetailsList.add(tapeStateDetails);
        });

        retval.setTotalStorageCapacity(totalStorage);
        retval.setAvailableStorageCapacity(availableStorage);
        retval.setUsedStorageCapacity(usedStorage);
        retval.setTapeCount(tapes.size());
        retval.setTapeTypeSummaries( CollectionFactory.toArray( TapeTypeSummaryApiBean.class, tapeTypeDetailsList ) );
        retval.setTapeStateSummaries( CollectionFactory.toArray( TapeStateSummaryApiBean.class, tapeStateDetailsList ) );

        return retval;
    }
}
