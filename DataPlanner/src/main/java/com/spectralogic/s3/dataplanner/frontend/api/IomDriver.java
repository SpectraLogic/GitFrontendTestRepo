package com.spectralogic.s3.dataplanner.frontend.api;

import com.spectralogic.util.shutdown.Shutdownable;

public interface IomDriver extends Shutdownable
{
	public void driveNewWork();
}
