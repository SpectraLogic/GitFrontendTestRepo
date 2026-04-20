/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.Marshalable;
import com.spectralogic.util.predicate.TruePredicate;

public final class GetBeansRetrieversRequestHandler extends BaseRequestHandler
{
    public GetBeansRetrieversRequestHandler()
    {
        super( new InternalAccessOnlyAuthenticationStrategy(), 
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.LIST, RestDomainType.BEANS_RETRIEVER ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final DatabaseContents retval = BeanFactory.newBean( DatabaseContents.class );
        final List< Type > types = new ArrayList<>();
        for ( final BeansRetriever< ? > service 
                : params.getServiceManager().getServices( new TruePredicate< Class< ? > >() ) )
        {
            final Type type = BeanFactory.newBean( Type.class );
            type.setDomainName( service.getServicedType().getName() );
            type.setBeansRetrieverName( service.getClass().getName() );
            try
            {
                type.setNumberOfType( Integer.valueOf( service.getCount() ) );
            }
            catch ( final Exception ex )
            {
                // some services won't support this call - that's fine
                Validations.verifyNotNull( "Shut up CodePro", ex );
                type.setNumberOfType( null );
            }
            types.add( type );
        }
        
        Collections.sort( types, new Comparator< Type >()
        {
            public int compare( Type o1, Type o2 )
            {
                return o1.getDomainName().compareTo( o2.getDomainName() );
            }
        } );
        
        retval.setTypes( CollectionFactory.toArray( Type.class, types ) );
        
        return BeanServlet.serviceGet( params, retval );
    }
    
    
    interface DatabaseContents extends SimpleBeanSafeToProxy, Marshalable
    {
        String TYPES = "types";
        
        public Type [] getTypes();
        
        public void setTypes( final Type [] types );
    }
    
    
    interface Type extends SimpleBeanSafeToProxy, Marshalable
    {
        String DOMAIN_NAME = "domainName";
        
        String getDomainName();
        
        void setDomainName( final String value );
        
        
        String BEANS_RETRIEVER_NAME = "beansRetrieverName";
        
        String getBeansRetrieverName();
        
        void setBeansRetrieverName( final String value );
        
        
        String NUMBER_OF_TYPE = "numberOfType";
        
        Integer getNumberOfType();
        
        void setNumberOfType( final Integer value );
    }
}
