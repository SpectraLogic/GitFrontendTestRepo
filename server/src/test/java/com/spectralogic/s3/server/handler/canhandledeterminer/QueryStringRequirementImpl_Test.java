/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class QueryStringRequirementImpl_Test
{

    @Test
    public void testMeetsRequirementReturnsTrueWhenNoParametersAndNoneRequired()
    {
        assertTrue(new QueryStringRequirementImpl().meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        new HashMap<String, String>() ) ), "Shoulda handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsFalseWhenNoParametersAndOneRequired()
    {
        assertFalse(new QueryStringRequirementImpl()
                .registerRequiredRequestParameters( RequestParameterType.JOB )
                .meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        new HashMap<String, String>() ) ), "Should notta handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsTrueWhenOneParameterAndOneRequired()
    {
        assertTrue(new QueryStringRequirementImpl()
                .registerRequiredRequestParameters( RequestParameterType.JOB )
                .meetsRequirement( mockDs3Request(
                        CollectionFactory.toSet( RequestParameterType.JOB ),
                        new HashMap<String, String>() ) ), "Shoulda handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsFalseWhenOneParameterAndADifferentOneRequired()
    {
        assertFalse(new QueryStringRequirementImpl()
                .registerRequiredRequestParameters( RequestParameterType.JOB )
                .meetsRequirement( mockDs3Request(
                        CollectionFactory.toSet( RequestParameterType.UPLOAD_ID ),
                        new HashMap<String, String>() ) ), "Should notta handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsFalseWhenOneParameterNoneRequired()
    {
        assertFalse(new QueryStringRequirementImpl()
                .meetsRequirement( mockDs3Request(
                        CollectionFactory.toSet( RequestParameterType.UPLOAD_ID ),
                        new HashMap<String, String>() ) ), "Should notta handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsTrueWhenNoParametersAndOneOptional()
    {
        assertTrue(new QueryStringRequirementImpl()
                .registerOptionalRequestParameters( RequestParameterType.JOB )
                .meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        new HashMap<String, String>() ) ), "Shoulda handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsTrueWhenOneParameterAndOneOptional()
    {
        assertTrue(new QueryStringRequirementImpl()
                .registerOptionalRequestParameters( RequestParameterType.JOB )
                .meetsRequirement( mockDs3Request(
                        CollectionFactory.toSet( RequestParameterType.JOB ),
                        new HashMap<String, String>() ) ), "Shoulda handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsFalseWhenOneParameterAndDifferentOneOptional()
    {
        assertFalse(new QueryStringRequirementImpl()
                .registerOptionalRequestParameters( RequestParameterType.JOB )
                .meetsRequirement( mockDs3Request(
                        CollectionFactory.toSet( RequestParameterType.UPLOAD_ID ),
                        new HashMap<String, String>() ) ), "Should notta handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsFalseWhenOneBeanPropertyAndNoneRequired()
    {
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put( TestBean.STRING_PROP, "bean_value" );
        assertFalse(new QueryStringRequirementImpl()
                .meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        beanProperties ) ), "Should notta handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsTrueWhenOneBeanPropertyAndOneRequired()
    {
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put( TestBean.STRING_PROP, "bean_value" );
        assertTrue(new QueryStringRequirementImpl()
                .registerDaoType( TestBean.class )
                .registerRequiredBeanProperties( TestBean.STRING_PROP )
                .meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        beanProperties ) ), "Shoulda handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsFalseWhenOneBeanPropertyAndDifferentOneRequired()
    {
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put( TestBean.STRING_PROP, "bean_value" );
        assertFalse(new QueryStringRequirementImpl()
                .registerDaoType( TestBean.class )
                .registerRequiredBeanProperties( TestBean.INT_PROP )
                .meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        beanProperties ) ), "Should notta handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsTrueWhenOneBeanPropertyAndOneOptional()
    {
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put( TestBean.STRING_PROP, "bean_value" );
        assertTrue(new QueryStringRequirementImpl()
                .registerDaoType( TestBean.class )
                .registerOptionalBeanProperties( TestBean.STRING_PROP )
                .meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        beanProperties ) ), "Shoulda handled the request.");
    }
    
    
    @Test
    public void testMeetsRequirementReturnsFalseWhenOneBeanPropertyAndDifferentOneOptional()
    {
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put( TestBean.STRING_PROP, "bean_value" );
        assertFalse(new QueryStringRequirementImpl()
                .registerDaoType( TestBean.class )
                .registerOptionalBeanProperties( TestBean.INT_PROP )
                .meetsRequirement( mockDs3Request(
                        new HashSet<RequestParameterType>(),
                        beanProperties ) ), "Should notta handled the request.");
    }
    
    
    @Test
    public void testRegisterDaoTypeMultipleTimesOnlyAllowedIfSameTypeRegistered()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl();
        queryStringRequirement.registerDaoType( TestBean.class );
        queryStringRequirement.registerDaoType( TestBean.class );
        queryStringRequirement.registerDaoType( TestBean.class );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            @Test
    public void test() throws Throwable
            {
                queryStringRequirement.registerDaoType( User.class );
            }
        } );
    }
    
    
    @Test
    public void testRegisterRequiredBeanPropertyThatDoesNotExistOnRegisteredDaoTypeNotAllowed()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
            .registerDaoType( TestBean.class );

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            @Test
    public void test() throws Throwable
            {
                queryStringRequirement.registerRequiredBeanProperties( 
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
            }
        } );
    }
    
    
    @Test
    public void testRegisterRequiredBeanPropertyWithoutDaoTypeRegistrationNotAllowed()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            @Test
    public void test() throws Throwable
            {
                queryStringRequirement.registerRequiredBeanProperties( 
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
            }
        } );
    }
    
    
    @Test
    public void testRegisterOptionalBeanPropertyThatDoesNotExistOnRegisteredDaoTypeNotAllowed()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
            .registerDaoType( TestBean.class );

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            @Test
    public void test() throws Throwable
            {
                queryStringRequirement.registerOptionalBeanProperties( 
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
            }
        } );
    }
    
    
    @Test
    public void testRegisterOptionalBeanPropertyWithoutDaoTypeRegistrationNotAllowed()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                queryStringRequirement.registerOptionalBeanProperties( 
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
            }
        } );
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersWithoutDaoTypeRegistrationNotAllowed()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
             public void test() throws Throwable
            {
                queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( new HashMap< String, String >() ),
                        AutoPopulatePropertiesWithDefaults.YES );
            }
        } );
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersReturnsExpectedBeanWhenAllRequiredStringsNonNull()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerRequiredBeanProperties( 
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.STRING_PROPERTY,
                "foobar" );
        beanProperties.put(
                QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                "foobar2" );
        beanProperties.put(
                QueryStringConstructableBean.INT_PROPERTY,
                "1234" );
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals("foobar", resultBean.getStringProperty(), "Shoulda had the expected string property.");
        assertEquals("foobar2", resultBean.getOptionalStringProperty(), "Shoulda had the expected string property.");
        assertEquals(1234,  resultBean.getIntProperty(), "Shoulda had the expected int property.");
        final Object expected = UUID.fromString( "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        assertEquals(expected, resultBean.getForeignKey(), "Shoulda had the expected foreign key.");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersReturnsExceptionWhenPropertyTypeHasWrongValue()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerRequiredBeanProperties( QueryStringConstructableBean.INT_PROPERTY );        
        final Map< String, String > beanProperties = new HashMap<>();
        final String nonIntValueForIntType = "not an int should throw exception";
        beanProperties.put( QueryStringConstructableBean.INT_PROPERTY, nonIntValueForIntType );
        
        final S3RestException ex = (S3RestException)TestUtil.assertThrows(
                "Invalid int value shoulda thrown exception when attempting to parse.", 
                GenericFailure.BAD_REQUEST,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                                mockParams( beanProperties ),
                                AutoPopulatePropertiesWithDefaults.YES );
                    }
                } );
        assertTrue(ex.getCause().getMessage().contains( nonIntValueForIntType ), "Error message should include the property value for easy troubleshhoting");
        assertTrue(ex.getCause().getCause().getClass().equals( NumberFormatException.class ), "Original exception should be included");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersThrowsIfPropertyTypeIsUUIDWeCannotParseAndDiscover()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = mockDaoDriver.createUser( "bob" );
        
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( Bucket.class )
                .registerRequiredBeanProperties( UserIdObservable.USER_ID );
        final String invalidUserOrUUID = "invalid user id and UUID";
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put( UserIdObservable.USER_ID, invalidUserOrUUID );
        
        final S3RestException ex = (S3RestException)TestUtil.assertThrows(
                "Illegal user name should notta been discoverable.", 
                GenericFailure.NOT_FOUND,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                                mockParams( dbSupport.getServiceManager(), beanProperties ),
                                AutoPopulatePropertiesWithDefaults.YES );
                    }
                } );

        assertTrue(ex.getCause().getMessage().contains( invalidUserOrUUID ), "Error message should include the property value for easy troubleshhoting");
        assertTrue(ex.getCause().getCause().getClass().equals( DaoException.class ), "Original exception should be included");

        beanProperties.put( UserIdObservable.USER_ID, user1.getName() );
        queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                mockParams( dbSupport.getServiceManager(), beanProperties ),
                AutoPopulatePropertiesWithDefaults.YES );
        
        beanProperties.put( UserIdObservable.USER_ID, UUID.randomUUID().toString() );
        queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                mockParams( dbSupport.getServiceManager(), beanProperties ),
                AutoPopulatePropertiesWithDefaults.YES );
    }


    @Test
    public void testGetBeanSpecifiedViaQueryParametersReturnsExpectedBeanWhenAllRequiredStringsNull()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerRequiredBeanProperties( 
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.STRING_PROPERTY,
                null );
        beanProperties.put(
                QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                null );
        beanProperties.put(
                QueryStringConstructableBean.INT_PROPERTY,
                "1234" );
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals("", resultBean.getStringProperty(), "Shoulda had the expected string property.");
        assertEquals("", resultBean.getOptionalStringProperty(), "Even thought this has the @Optional annotation, because it is registered as required, " 
                + "we convert NULL to empty string.");
        assertEquals(1234,  resultBean.getIntProperty(), "Shoulda had the expected int property.");
        final Object expected = UUID.fromString( "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        assertEquals(expected, resultBean.getForeignKey(), "Shoulda had the expected foreign key.");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersReturnsExpectedBeanWhenAllRequiredNotAllSpecified()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerRequiredBeanProperties(
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.STRING_PROPERTY,
                null );
        beanProperties.put(
                QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                null );
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        TestUtil.assertThrows( null, AWSFailure.INVALID_ARGUMENT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
            }
        } );
    }
    

    @Test
    public void testGetBeanSpecifiedViaQueryParametersReturnsExpectedBeanWhenAllOptionalStringsNonNull()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerOptionalBeanProperties(
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.STRING_PROPERTY,
                "foobar" );
        beanProperties.put(
                QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                "foobar2" );
        beanProperties.put(
                QueryStringConstructableBean.INT_PROPERTY,
                "1234" );
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals("foobar", resultBean.getStringProperty(), "Shoulda had the expected string property.");
        assertEquals("foobar2", resultBean.getOptionalStringProperty(), "Shoulda had the expected string property.");
        assertEquals(1234,  resultBean.getIntProperty(), "Shoulda had the expected int property.");
        final Object expected = UUID.fromString( "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        assertEquals(expected, resultBean.getForeignKey(), "Shoulda had the expected foreign key.");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersRequiredTypeStringConvertNullToEmpty()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerRequiredBeanProperties(  
                        QueryStringConstructableBean.STRING_PROPERTY
                        );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.STRING_PROPERTY,
                null );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals("", resultBean.getStringProperty(), "Shoulda had the expected string property.");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersOptionalTypeStringDoesntConvertNullToEmpty()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
            .registerDaoType( QueryStringConstructableBean.class )
            .registerOptionalBeanProperties(   
                QueryStringConstructableBean.STRING_PROPERTY
                );

        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.STRING_PROPERTY,
                null );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals("", resultBean.getStringProperty(), "Because it is required property shoulda converted null value to empty string.");
        assertEquals(null, resultBean.getOptionalStringProperty(), "Because it is optional property should not convert null value to empty string.");
    }

        
    @Test
    public void testGetBeanSpecifiedViaQueryParametersNonStringNonOptionalHasNullValueThrowException()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerOptionalBeanProperties(
                        QueryStringConstructableBean.INT_PROPERTY);
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.INT_PROPERTY,
                null );
        
        final Throwable ex = TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
            }
        } );
        assertTrue(ex.getMessage().startsWith( "Failed to set property" ), "Should start with \"Failed to set property\".");
        assertTrue(ex.getCause().getMessage().startsWith( "The property" )
                && ex.getCause().getMessage().contains( "cannot be null." ), "Should contain.");
        assertEquals(ex.getClass(), S3RestException.class, "Shoulda had the expected cause exception.");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersNonStringIsOptionalHasNullValueThrowsException()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerOptionalBeanProperties(
                        QueryStringConstructableBean.OPTIONAL_INT_PROPERTY);
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.OPTIONAL_INT_PROPERTY,
                null );
        
        final Throwable ex = TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
            }
        } );
        assertTrue(ex.getMessage().startsWith( "Failed to set property" ), "Should start with \"Failed to set property\".");
        assertTrue(ex.getCause().getMessage().startsWith( "The property" )
                && ex.getCause().getMessage().contains( "cannot be null." ), "Should contain.");
        assertEquals(ex.getClass(), S3RestException.class, "Shoulda had the expected cause exception.");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersRequiredNonStringHasNullValueThrowsException()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerRequiredBeanProperties( 
                        QueryStringConstructableBean.OPTIONAL_INT_PROPERTY);
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.OPTIONAL_INT_PROPERTY,
                null );
        
        final Throwable ex = TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
            }
        } );
        assertTrue(ex.getMessage().startsWith( "Failed to set property" ), "Should start with \"Failed to set property\".");
        assertTrue(ex.getCause().getMessage().startsWith( "The property" )
                && ex.getCause().getMessage().contains( "cannot be null." ), "Should contain.");
        assertEquals(ex.getClass(), S3RestException.class, "Shoulda had the expected cause exception.");
    }
    
    
    @Test
    public void testGetBeanSpecifiedViaQueryParametersReturnsExpectedBeanWhenAllOptionalStringsNull()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerOptionalBeanProperties(
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.STRING_PROPERTY,
                null );
        beanProperties.put(
                QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                null );
        beanProperties.put(
                QueryStringConstructableBean.INT_PROPERTY,
                "1234" );
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals("", resultBean.getStringProperty(), "We expect empty because string prop isn't optional");
        assertEquals(null, resultBean.getOptionalStringProperty(), "Shoulda had the expected string property.");
        assertEquals(1234,  resultBean.getIntProperty(), "Shoulda had the expected int property.");
        final Object expected = UUID.fromString( "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        assertEquals(expected, resultBean.getForeignKey(), "Shoulda had the expected foreign key.");
    }
    

    @Test
    public void testGetBeanSpecifiedViaQueryParametersReturnsExpectedBeanWhenAllOptionalNotAllProvided()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerOptionalBeanProperties( 
                        QueryStringConstructableBean.STRING_PROPERTY,
                        QueryStringConstructableBean.OPTIONAL_STRING_PROPERTY,
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.FOREIGN_KEY );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals(null, resultBean.getStringProperty(), "Shoulda had the expected string property.");
        assertEquals(null, resultBean.getOptionalStringProperty(), "Shoulda had the expected string property.");
        assertEquals(0,  resultBean.getIntProperty(), "Shoulda had the expected int property.");
        final Object expected = UUID.fromString( "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        assertEquals(expected, resultBean.getForeignKey(), "Shoulda had the expected foreign key.");
    }
    

    @Test
    public void testGetBeanSpecifiedViaQueryParametersPopulatesDefaultsWhenAutoPopulateWithDefaultsTrue()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerOptionalBeanProperties(
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.INT_WITH_DEFAULT );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.YES );
        assertEquals(0,  resultBean.getIntProperty(), "Shoulda had the expected int property.");
        assertEquals(7,  resultBean.getIntWithDefault(), "Shoulda had the expected int property.");
    }
    

    @Test
    public void testGetBeanSpecifiedViaQueryParametersDoesNotPopulateDefaultsWhenAutoPopulateFalse()
    {
        final QueryStringRequirement queryStringRequirement = new QueryStringRequirementImpl()
                .registerDaoType( QueryStringConstructableBean.class )
                .registerOptionalBeanProperties(
                        QueryStringConstructableBean.INT_PROPERTY,
                        QueryStringConstructableBean.INT_WITH_DEFAULT );
        
        final Map< String, String > beanProperties = new HashMap<>();
        beanProperties.put(
                QueryStringConstructableBean.FOREIGN_KEY,
                "974d17e5-ddd5-4eb6-a8c3-2e9ec170108f" );
        final QueryStringConstructableBean resultBean =
                (QueryStringConstructableBean)queryStringRequirement.getBeanSpecifiedViaQueryParameters(
                        mockParams( beanProperties ),
                        AutoPopulatePropertiesWithDefaults.NO );
        assertEquals(0,  resultBean.getIntProperty(), "Shoulda had the expected int property.");
        assertEquals(0,  resultBean.getIntWithDefault(), "Shoulda had the expected int property.");
    }
    
    
    private static CommandExecutionParams mockParams( final Map< String, String > beanProperties )
    {
        return mockParams( new MockBeansServiceManager(), beanProperties );
    }
    
    
    private static CommandExecutionParams mockParams(
            final BeansServiceManager serviceManager,
            final Map< String, String > beanProperties )
    {
        final Method getServiceManagerMethod;
        final Method getRequestMethod;
        try
        {
            getServiceManagerMethod = CommandExecutionParams.class.getMethod( "getServiceManager" );
            getRequestMethod = CommandExecutionParams.class.getMethod( "getRequest" );
        }
        catch ( final NoSuchMethodException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
        InvocationHandler invocationHandler = MockInvocationHandler.forMethod(
                getServiceManagerMethod,
                new ConstantResponseInvocationHandler( serviceManager ),
                null );
        invocationHandler = MockInvocationHandler.forMethod(
                getRequestMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return mockDs3Request( new HashSet< RequestParameterType >(), beanProperties );
                    }
                },
                invocationHandler );
        return InterfaceProxyFactory.getProxy( CommandExecutionParams.class, invocationHandler );
    }

    
    private static DS3Request mockDs3Request(
            final Set< RequestParameterType > requestParameters,
            final Map< String, String > beanProperties )
    {
        final Method getRequestParametersMethod;
        final Method getBeanPropertyValueMapFromRequestParametersMethod;
        try
        {
            getRequestParametersMethod = DS3Request.class.getMethod( "getRequestParameters" );
            getBeanPropertyValueMapFromRequestParametersMethod =
                    DS3Request.class.getMethod( "getBeanPropertyValueMapFromRequestParameters" );
        }
        catch ( final NoSuchMethodException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
        InvocationHandler invocationHandler = MockInvocationHandler.forMethod(
                getRequestParametersMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return requestParameters;
                    }
                },
                null );
        invocationHandler = MockInvocationHandler.forMethod(
                getBeanPropertyValueMapFromRequestParametersMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return beanProperties;
                    }
                },
                invocationHandler );
        return InterfaceProxyFactory.getProxy( DS3Request.class, invocationHandler  );
    }
    
    
    public static interface QueryStringConstructableBean extends SimpleBeanSafeToProxy
    {
        String STRING_PROPERTY = "stringProperty";
        
        String getStringProperty();
        
        void setStringProperty( final String value );
        
        
        String OPTIONAL_STRING_PROPERTY = "optionalStringProperty";
        
        @Optional
        String getOptionalStringProperty();
        
        void setOptionalStringProperty( final String value );
        
        
        String INT_PROPERTY = "intProperty";
        
        int getIntProperty();
        
        void setIntProperty( final int value );
        
        
        String OPTIONAL_INT_PROPERTY = "optionalIntProperty";
        
        @Optional
        int getOptionalIntProperty();
        
        void setOptionalIntProperty( final int value );

        
        String FOREIGN_KEY = "foreignKey";
        
        UUID getForeignKey();
        
        void setForeignKey( final UUID value );
        
        
        String INT_WITH_DEFAULT = "intWithDefault";
        
        @DefaultIntegerValue( 7 )
        int getIntWithDefault();
        
        void setIntWithDefault( final int value );
    }// end inner class
}
