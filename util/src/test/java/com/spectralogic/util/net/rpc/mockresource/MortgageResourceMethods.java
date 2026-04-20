/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import java.util.UUID;

import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;

public interface MortgageResourceMethods extends RpcResource
{
    @RpcMethodReturnType( AllMortgagesResponse.class )
    RpcFuture< AllMortgagesResponse > getAllMortgages();
    

    @RpcMethodReturnType( AllMortgagesResponse.class )
    RpcFuture< AllMortgagesResponse > getAllMortgagesWithBadResponse(
            @NullAllowed final AllMortgagesResponse ignoredParam );
    

    @NullAllowed
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > getMortgageFor( final UUID clientId );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > addPaymentForMortgage( final UUID clientId, final UUID mortgageId, final int amount );
    

    @RpcMethodReturnType( Integer.class )
    RpcFuture< Integer > getSum( @NullAllowed final int ... values );
    

    @RpcMethodReturnType( Integer.class )
    RpcFuture< Integer > getMax( final Integer ... values );
    

    @RpcMethodReturnType( Integer.class )
    RpcFuture< Integer > performMath( final MathOperation operation, final Integer [] values );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > badMethodThatReturnsSomething();
    
    
    @RpcMethodReturnType( UUID.class )
    RpcFuture< Integer > badMethodThatReturnsWrongType();
    
    
    boolean isBadMethodThatDoesNotReturnRpcFutureAndThusCannotBeCalledByRpcClient();
    
    
    public enum MathOperation
    {
        SUM,
        MAX
    }
}
