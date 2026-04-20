/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.shared;

import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionFailureService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ActiveFailuresImpl_Test 
{
    @Test
    public void testConstructorNullServiceNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new ActiveFailuresImpl<>( null, BeanFactory.newBean( TapePartitionFailure.class ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullBaseBeanNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
             public void test()
            {
                new ActiveFailuresImpl<>( 
                        dbSupport.getServiceManager().getService( TapePartitionFailureService.class ), 
                        null );
            }
        } );
    }
    
    
    @Test
    public void testCommitDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID p1 = mockDaoDriver.createTapePartition( null, "a" ).getId();
        final UUID p2 = mockDaoDriver.createTapePartition( null, "b" ).getId();
        
        final TapePartitionFailureService service = 
                dbSupport.getServiceManager().getService( TapePartitionFailureService.class );
        final TapePartitionFailure f1 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p1 )
                .setType( TapePartitionFailureType.values()[ 0 ] ).setErrorMessage( "a" );
        final TapePartitionFailure f2 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p1 )
                .setType( TapePartitionFailureType.values()[ 0 ] ).setErrorMessage( "b" );
        final TapePartitionFailure f3 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p1 )
                .setType( TapePartitionFailureType.values()[ 1 ] ).setErrorMessage( "c" );
        final TapePartitionFailure f4 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p1 )
                .setType( TapePartitionFailureType.values()[ 2 ] ).setErrorMessage( "d" );
        final TapePartitionFailure f5 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p2 )
                .setType( TapePartitionFailureType.values()[ 2 ] ).setErrorMessage( "a" );
        final TapePartitionFailure f6 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p2 )
                .setType( TapePartitionFailureType.values()[ 2 ] ).setErrorMessage( "b" );
        final TapePartitionFailure f7 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p2 )
                .setType( TapePartitionFailureType.values()[ 3 ] ).setErrorMessage( "c" );
        final TapePartitionFailure f8 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p2 )
                .setType( TapePartitionFailureType.values()[ 4 ] ).setErrorMessage( "d" );
        final TapePartitionFailure f9 = BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p2 )
                .setType( TapePartitionFailureType.values()[ 4 ] ).setErrorMessage( "d" );
        
        for ( final TapePartitionFailure f 
                : CollectionFactory.toSet( f1, f2, f3, f4, f5, f6, f7, f8, f9 ) )
        {
            dbSupport.getDataManager().createBean( f );
        }
        
        final UUID id1 = f1.getId();
        final UUID id2 = f2.getId();
        final UUID id3 = f3.getId();
        final UUID id4 = f4.getId();
        final UUID id5 = f5.getId();
        final UUID id6 = f6.getId();
        final UUID id7 = f7.getId();
        final UUID id8 = f8.getId();
        final UUID id9 = f9.getId();
        Set< UUID > originalFailures = 
                CollectionFactory.toSet( id1, id2, id3, id4, id5, id6, id7, id8, id9 );
        
        ActiveFailures activeFailures = new ActiveFailuresImpl<>(
                service,
                BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p1 )
                .setType( TapePartitionFailureType.values()[ 0 ] ) );
        activeFailures.add( "a" );
        activeFailures.add( "b" );
        activeFailures.commit();
        Set< UUID > newFailures = BeanUtils.toMap( service.retrieveAll( Require.not(
                Require.beanPropertyEqualsOneOf( Identifiable.ID, originalFailures ) ) ).toSet() ).keySet();
        assertEquals(0,  newFailures.size(), "Should notta created any failures.");
        assertEquals(originalFailures, BeanUtils.toMap( service.retrieveAll().toSet() ).keySet(), "Should notta made any changes.");

        activeFailures = new ActiveFailuresImpl<>(
                service,
                BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p1 )
                .setType( TapePartitionFailureType.values()[ 0 ] ) );
        activeFailures.add( "a" );
        activeFailures.commit();
        newFailures = BeanUtils.toMap( service.retrieveAll( Require.not(
                Require.beanPropertyEqualsOneOf( Identifiable.ID, originalFailures ) ) ).toSet() ).keySet();
        assertEquals(0,  newFailures.size(), "Should notta created any failures.");
        originalFailures =
                CollectionFactory.toSet( id1, id3, id4, id5, id6, id7, id8, id9 );
        assertEquals(originalFailures, BeanUtils.toMap( service.retrieveAll().toSet() ).keySet(), "Shoulda deleted single failure that went away made any changes.");

        activeFailures = new ActiveFailuresImpl<>( 
                service,
                BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( p1 )
                .setType( TapePartitionFailureType.values()[ 0 ] ) );
        activeFailures.add( "a" );
        activeFailures.add( "b" );
        activeFailures.commit();
        newFailures = BeanUtils.toMap( service.retrieveAll( Require.not(
                Require.beanPropertyEqualsOneOf( Identifiable.ID, originalFailures ) ) ).toSet() ).keySet();
        assertEquals(1,  newFailures.size(), "Shoulda created single failure.");
        originalFailures =
                CollectionFactory.toSet(
                        id1, id3, id4, id5, id6, id7, id8, id9, newFailures.iterator().next() );
        assertEquals(originalFailures, BeanUtils.toMap( service.retrieveAll().toSet() ).keySet(), "Shoulda created single failure.");
    }
}
