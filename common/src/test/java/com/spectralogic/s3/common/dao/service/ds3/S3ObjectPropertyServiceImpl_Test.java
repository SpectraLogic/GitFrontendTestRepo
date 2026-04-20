/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class S3ObjectPropertyServiceImpl_Test 
{
    @Test
    public void testCreatePropertiesWithoutCollisionsDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3ObjectProperty prop1 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "a" ).setValue( "va" );
        final S3ObjectProperty prop2 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "b" ).setValue( "vb" );
        final S3ObjectProperty prop3 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "a" ).setValue( "va" );
        
        final S3ObjectPropertyService service = 
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        service.createProperties( o1.getId(), CollectionFactory.toList( prop1, prop2 ) );
        service.createProperties( o2.getId(), CollectionFactory.toList( prop3 ) );

        assertEquals(2,  service.getCount(Require.beanPropertyEquals(S3ObjectProperty.OBJECT_ID, o1.getId())), "Shoulda created all properties.");
        assertEquals(1,  service.getCount(Require.beanPropertyEquals(S3ObjectProperty.OBJECT_ID, o2.getId())), "Shoulda created all properties.");
    }
    
    
    @Test
    public void testCreatePropertiesWithKeyNameCollisionNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3ObjectProperty prop1 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "a" ).setValue( "va" );
        final S3ObjectProperty prop2 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "a" ).setValue( "vb" );
        
        final S3ObjectPropertyService service = 
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test() throws Throwable
                    {
                        service.createProperties( o1.getId(), CollectionFactory.toList( prop1, prop2 ) );
                    }
                } );
    }
    
    
    @Test
    public void testCreatePropertiesWithNullValueNotAllowedBLKP3471()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3ObjectProperty prop1 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "keyWithNullValue" ).setValue( null );
        
        final S3ObjectPropertyService service =
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST,
                ( ) -> service.createProperties( o1.getId(), CollectionFactory.toList( prop1 ) ) );
    }
    
    
    @Test
    public void testCreatePropertiesWithSpectraNamespaceCollisionNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3ObjectProperty prop1 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "a" ).setValue( "va" );
        final S3ObjectProperty prop2 = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "x-spectra-something" ).setValue( "vb" );
        
        final S3ObjectPropertyService service = 
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.createProperties( o1.getId(), CollectionFactory.toList( prop1, prop2 ) );
            }
        } );
    }
    
    
    @Test
    public void testPopulateObjectHttpHeadersDoesNotAddQuotesWhenNotNecessary()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final S3ObjectPropertyService service =
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        dbSupport.getServiceManager().getService( BlobService.class ).update( 
                mockDaoDriver.getBlobFor( o2.getId() )
                .setChecksum( "ccrc" ).setChecksumType( ChecksumType.MD5 ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.createObjectProperties( 
                o1.getId(), 
                CollectionFactory.toMap( "key", "value" ) );
        mockDaoDriver.createObjectProperties( 
                o2.getId(),
                CollectionFactory.toMap( 
                        S3HeaderType.ETAG.getHttpHeaderName(), "value" ) );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final HttpServletResponse response =
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, btih );
        service.populateObjectHttpHeaders( o1.getId(), response );
        assertEquals(1,  btih.getTotalCallCount(), "Shoulda set single response header.");
        assertEquals("key", btih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda set single response header.");
        assertEquals("value", btih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ), "Shoulda set single response header.");
    }
    
    
    @Test
    public void testPopulateObjectHttpHeadersAddsQuotesWhenNecessary()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final S3ObjectPropertyService service =
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        dbSupport.getServiceManager().getService( BlobService.class ).update( 
                mockDaoDriver.getBlobFor( o2.getId() )
                .setChecksum( "ccrc" ).setChecksumType( ChecksumType.MD5 ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.createObjectProperties( 
                o1.getId(), 
                CollectionFactory.toMap( "key", "value" ) );
        mockDaoDriver.createObjectProperties( 
                o2.getId(),
                CollectionFactory.toMap( 
                        S3HeaderType.ETAG.getHttpHeaderName(), "value" ) );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final HttpServletResponse response =
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, btih );
        service.populateObjectHttpHeaders( o2.getId(), response );
        assertEquals(1,  btih.getTotalCallCount(), "Shoulda set single response header.");
        assertEquals("ETag", btih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda set single response header.");
        assertEquals("\"value\"", btih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ), "Shoulda set single response header.");
    }
    
    
    @Test
    public void testPopulateAllHttpHeadersDoesNotAddQuotesWhenNotNecessary()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final S3ObjectPropertyService service =
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObjectStub( null, "o1", 10 );
        final S3Object o2 = mockDaoDriver.createObjectStub( null, "o2", 10 );
        dbSupport.getServiceManager().getService( BlobService.class ).update( 
                mockDaoDriver.getBlobFor( o2.getId() )
                .setChecksum( "ccrc" ).setChecksumType( ChecksumType.MD5 ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.createObjectProperties( 
                o1.getId(), 
                CollectionFactory.toMap( "key", "value" ) );
        mockDaoDriver.createObjectProperties( 
                o2.getId(),
                CollectionFactory.toMap( 
                        S3HeaderType.ETAG.getHttpHeaderName(), "value" ) );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final HttpServletResponse response =
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, btih );
        service.populateAllHttpHeaders( mockDaoDriver.getBlobFor( o1.getId() ).getId(), response );
        assertEquals(1,  btih.getTotalCallCount(), "Shoulda set single response header.");
        assertEquals("key", btih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda set single response header.");
        assertEquals("value", btih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ), "Shoulda set single response header.");
    }
    

    @Test
    public void testPopulateAllHttpHeadersAddsQuotesWhenNecessary()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final S3ObjectPropertyService service =
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        dbSupport.getServiceManager().getService( BlobService.class ).update( 
                mockDaoDriver.getBlobFor( o2.getId() )
                .setChecksum( "ccrc" ).setChecksumType( ChecksumType.MD5 ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.createObjectProperties( 
                o1.getId(), 
                CollectionFactory.toMap( "key", "value" ) );
        mockDaoDriver.createObjectProperties( 
                o2.getId(),
                CollectionFactory.toMap( 
                        S3HeaderType.ETAG.getHttpHeaderName(), "value" ) );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final HttpServletResponse response =
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, btih );
        service.populateAllHttpHeaders( mockDaoDriver.getBlobFor( o2.getId() ).getId(), response );

        assertEquals(3,  btih.getTotalCallCount(), "Shoulda had 3 method calls.");
        assertEquals("Content-MD5", btih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda checked for existence of Checksum header key.");
        assertEquals("Content-MD5", btih.getMethodInvokeData().get( 1 ).getArgs().get( 0 ), "Shoulda set single response header key.");
        assertEquals("ccrc", btih.getMethodInvokeData().get( 1 ).getArgs().get( 1 ), "Shoulda set single response header value.");
        assertEquals("ETag", btih.getMethodInvokeData().get( 2 ).getArgs().get( 0 ), "Shoulda set 2nd single response header key.");
        assertEquals("\"value\"", btih.getMethodInvokeData().get( 2 ).getArgs().get( 1 ), "Shoulda set 2nd single response header value.");
    }
    
    
    @Test
    public void testDeleteTemporaryCreationDatesDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final S3ObjectPropertyService service =
                dbSupport.getServiceManager().getService( S3ObjectPropertyService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        
        final Set< S3ObjectProperty > props = new HashSet<>();
        props.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setObjectId( o1.getId() )
                .setKey( "a" )
                .setValue( "1" ) );
        props.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setObjectId( o1.getId() )
                .setKey( "b" )
                .setValue( "2" ) );
        props.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setObjectId( o1.getId() )
                .setKey( KeyValueObservable.CREATION_DATE )
                .setValue( "1000" ) );
        props.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setObjectId( o2.getId() )
                .setKey( "a" )
                .setValue( "1" ) );
        props.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setObjectId( o2.getId() )
                .setKey( KeyValueObservable.CREATION_DATE )
                .setValue( "1000" ) );
        props.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setObjectId( o3.getId() )
                .setKey( KeyValueObservable.CREATION_DATE )
                .setValue( "1000" ) );
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( S3ObjectPropertyService.class ).create( props );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        service.deleteTemporaryCreationDates( CollectionFactory.toSet( o1.getId(), o2.getId() ) );
        assertEquals(2,  service.getCount(S3ObjectProperty.OBJECT_ID, o1.getId()), "Shoulda deleted temp creation date record.");
        assertEquals(1,  service.getCount(S3ObjectProperty.OBJECT_ID, o2.getId()), "Shoulda deleted temp creation date record.");
        assertEquals(1,  service.getCount(S3ObjectProperty.OBJECT_ID, o3.getId()), "Should notta deleted temp creation date record.");
    }
}
