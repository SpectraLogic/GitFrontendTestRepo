/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.mockresource.BadConcreteResource;
import com.spectralogic.util.net.rpc.mockresource.BadResource;
import com.spectralogic.util.net.rpc.mockresource.MortgageResource;
import com.spectralogic.util.net.rpc.mockresource.MortgageResourceImpl;
import com.spectralogic.util.net.rpc.mockresource.MortgageResourceMethods;
import com.spectralogic.util.net.rpc.mockresource.Resource1;
import com.spectralogic.util.net.rpc.mockresource.Resource1Impl;
import com.spectralogic.util.net.rpc.mockresource.UserResource;
import com.spectralogic.util.net.rpc.mockresource.UserResourceImpl;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public class RpcResourceUtil_Test 
{

    @Test
    public void testGetApiNullApiNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    RpcResourceUtil.getApi( null );
                }
            } );
    }
    
    
    @Test
    public void testGetApiThatIsNotAnRpcResourceNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                final Class< ? > clazz = String.class;
                @SuppressWarnings( "unchecked" )
                final Class< ? extends RpcResource > castedClass = (Class< ? extends RpcResource >)clazz;
                RpcResourceUtil.getApi( castedClass );
            }
        } );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                final Class< ? > clazz = HashSet.class;
                @SuppressWarnings( "unchecked" )
                final Class< ? extends RpcResource > castedClass = (Class< ? extends RpcResource >)clazz;
                RpcResourceUtil.getApi( castedClass );
            }
        } );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    final Class< ? > clazz = Set.class;
                    @SuppressWarnings( "unchecked" )
                    final Class< ? extends RpcResource > castedClass = (Class< ? extends RpcResource >)clazz;
                    RpcResourceUtil.getApi( castedClass );
                }
        } );
    }
    
    
    @Test
    public void testGetApiThatHasDuplicateRpcResourceNameAnnotationsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    RpcResourceUtil.getApi( BadResource.class );
                }
            } );
    }
    
    
    @Test
    public void testGetApiThatHasNonInterfaceApiDefinitionNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    RpcResourceUtil.getApi( BadConcreteResource.class );
                }
            } );
    }
    
    
    @Test
    public void testGetApiThatHasNoResourceNameAnnotationNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    RpcResourceUtil.getApi( MortgageResourceMethods.class );
                }
            } );
    }
    
    
    @Test
    public void testGetApiDoesSo()
    {
        assertEquals(MortgageResource.class, RpcResourceUtil.getApi( MortgageResourceImpl.class ), "Shoulda found api.");
        assertEquals(Resource1.class, RpcResourceUtil.getApi( Resource1.class ), "Shoulda found api.");
        assertEquals(Resource1.class, RpcResourceUtil.getApi( Resource1Impl.class ), "Shoulda found api.");
        assertEquals(UserResource.class, RpcResourceUtil.getApi( UserResource.class ), "Shoulda found api.");
        assertEquals(UserResource.class, RpcResourceUtil.getApi( UserResourceImpl.class ), "Shoulda found api.");
    }
    
    
    @Test
    public void testValidateApiNullApiNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                RpcResourceUtil.validate( null );
            }
        } );
    }
    
    
    @Test
    public void testValidateApiPassesForValidApis()
    {
        RpcResourceUtil.validate( MortgageResource.class );
        RpcResourceUtil.validate( MortgageResourceImpl.class );
        RpcResourceUtil.validate( Resource1.class );
        RpcResourceUtil.validate( Resource1Impl.class );
        RpcResourceUtil.validate( UserResource.class );
        RpcResourceUtil.validate( UserResourceImpl.class );
        RpcResourceUtil.validate( ProperlyFormedResourceApi.class );
    }
    
    
    @RpcResourceName( "property_formed" )
    public interface ProperlyFormedResourceApi extends MortgageResourceMethods
    {
        @RpcMethodReturnType( void.class )
        RpcFuture< Object > someMethod();
    }
    
    
    @Test
    public void testValidateApiFailsWhenRpcMethodDefinedThatDoesNotAnnotateItsReturnType()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                RpcResourceUtil.validate( MalformedResourceApiMissingReturnTypeAnnotation.class );
            }
        } );
    }
    
    
    @RpcResourceName( "malformed" )
    public interface MalformedResourceApiMissingReturnTypeAnnotation extends MortgageResourceMethods
    {
        RpcFuture< Object > someMethod();
    }
    
    
    @Test
    public void testValidateApiFailsWhenAdditionalRpcMethodsDefinedBeyondInterfaceWithRpcResourceName()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                RpcResourceUtil.validate( MalformedResourceApiDefiningAdditionalRpcMethods.class );
            }
        } );
    }
    
    
    public interface MalformedResourceApiDefiningAdditionalRpcMethods extends MortgageResource
    {
        @RpcMethodReturnType( void.class )
        RpcFuture< Object > someMethod();
    }
    
    
    @Test
    public void testValidateApiFailsWhenResourceDoesNotExtendFromRpcResourceInterface()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                final Class< ? > clazz = MalformedResourceApiNotExtendingFromRpcResource.class;
                @SuppressWarnings( "unchecked" )
                final Class< ? extends RpcResource > castedClass = (Class< ? extends RpcResource >)clazz;
                RpcResourceUtil.validate( castedClass );
            }
        } );
    }
    
    
    public interface MalformedResourceApiNotExtendingFromRpcResource
    {
        @RpcMethodReturnType( void.class )
        RpcFuture< Object > someMethod();
    }
    
    
    @Test
    public void testGetResourceNameReturnsResourceName()
    {
        assertEquals("mortgage_processor", RpcResourceUtil.getResourceName( MortgageResource.class ), "Shoulda determined resource name correctly.");
        assertEquals("mortgage_processor", RpcResourceUtil.getResourceName( MortgageResourceImpl.class ), "Shoulda determined resource name correctly.");
        assertEquals("resource1", RpcResourceUtil.getResourceName( Resource1.class ), "Shoulda determined resource name correctly.");
        assertEquals("resource1", RpcResourceUtil.getResourceName( Resource1Impl.class ), "Shoulda determined resource name correctly.");
        assertEquals("user_resource", RpcResourceUtil.getResourceName( UserResource.class ), "Shoulda determined resource name correctly.");
        assertEquals("user_resource", RpcResourceUtil.getResourceName( UserResourceImpl.class ), "Shoulda determined resource name correctly.");
        assertEquals("property_formed", RpcResourceUtil.getResourceName( ProperlyFormedResourceApi.class ), "Shoulda determined resource name correctly.");
    }
    
    
    @Test
    public void testValidateResponseAgainstPrimitiveTypesAlwaysPasses()
    {
        RpcResourceUtil.validateResponse( null, GenericFailure.INTERNAL_ERROR );
        RpcResourceUtil.validateResponse( Integer.valueOf( 2 ), GenericFailure.INTERNAL_ERROR );
        RpcResourceUtil.validateResponse( "hi", GenericFailure.INTERNAL_ERROR );
    }
    
    
    @Test
    public void testValidateResponseAgainstBeanPassesIfAllRequiredParametersAreSupplied()
    {
        final TestBean bean = getPopulatedTestBean();
        RpcResourceUtil.validateResponse( bean, GenericFailure.INTERNAL_ERROR );
        
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                bean.setObjectIntProp( null );
                RpcResourceUtil.validateResponse( bean, GenericFailure.INTERNAL_ERROR );
            }
        } );
    }
    
    
    @Test
    public void testValidateResponseAgainstNestedBeanPassesIfAllRequiredParametersAreSupplied()
    {
        final TestBean bean = getPopulatedTestBean();
        bean.setNestedBean( getPopulatedTestBean() );
        RpcResourceUtil.validateResponse( bean, GenericFailure.INTERNAL_ERROR );
        
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                bean.getNestedBean().setObjectIntProp( null );
                RpcResourceUtil.validateResponse( bean, GenericFailure.INTERNAL_ERROR );
            }
        } );
    }
    
    
    private TestBean getPopulatedTestBean()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        bean.setStringProp( "a" );
        bean.setObjectLongProp( Long.valueOf( 2 ) );
        bean.setObjectIntProp( Integer.valueOf( 2 ) );
        bean.setObjectBooleanProp( Boolean.TRUE );
        bean.setSetProp( new HashSet< String >() );
        bean.setListProp( new ArrayList< String >() );
        bean.setArrayProp( (String[])Array.newInstance( String.class, 0 ) );
        return bean;
    }
}
