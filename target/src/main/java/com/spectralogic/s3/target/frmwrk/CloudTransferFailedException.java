package com.spectralogic.s3.target.frmwrk;

public class CloudTransferFailedException extends Exception
{
	private static final long serialVersionUID = -1141159576906903165L;


	public CloudTransferFailedException( final String message, final Exception cause )
	{
		super( message, cause );
	}
	
	
	public CloudTransferFailedException( final String message )
	{
		super( message );
	}
	
	
	public CloudTransferFailedException( final Exception cause )
	{
		super( cause );
	}

}
